/*
 * Copyright (c) 2024. The BifroMQ Authors. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.bifromq.baserpc.trafficgovernor;

import static org.apache.bifromq.baserpc.trafficgovernor.SharedScheduler.RPC_SHARED_SCHEDULER;
import static java.util.Collections.emptySet;

import org.apache.bifromq.basecrdt.service.ICRDTService;
import org.apache.bifromq.basehlc.HLC;
import org.apache.bifromq.baserpc.proto.RPCServer;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import io.grpc.inprocess.InProcessSocketAddress;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.subjects.BehaviorSubject;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class RPCServiceTrafficManager extends RPCServiceAnnouncer
    implements IRPCServiceServerRegister, IRPCServiceLandscape, IRPCServiceTrafficGovernor {
    private final AtomicBoolean closed = new AtomicBoolean();
    private final CompositeDisposable disposables = new CompositeDisposable();
    private final BehaviorSubject<Set<ServerEndpoint>> serverEndpointSubject =
        BehaviorSubject.createDefault(emptySet());

    public RPCServiceTrafficManager(String serviceUniqueName, ICRDTService crdtService) {
        super(serviceUniqueName, crdtService);
        disposables.add(Observable.combineLatest(announcedServers(), aliveAnnouncers(), this::refreshAliveServerList)
            .observeOn(RPC_SHARED_SCHEDULER)
            .subscribe(serverEndpointSubject::onNext));
    }

    @Override
    public Observable<Map<String, Map<String, Integer>>> trafficRules() {
        Preconditions.checkState(!closed.get());
        return super.trafficRules();
    }

    @Override
    public Observable<Set<ServerEndpoint>> serverEndpoints() {
        Preconditions.checkState(!closed.get());
        return serverEndpointSubject;
    }

    @Override
    public IServerRegistration reg(String id, InetSocketAddress hostAddr, Set<String> groupTags,
                                   Map<String, String> attrs) {
        Preconditions.checkState(!closed.get());
        return new ServerRegistration(RPCServer.newBuilder()
            .setAgentHostId(crdtService.agentHostId())
            .setId(id)
            .setHost(hostAddr.getAddress().getHostAddress())
            .setPort(hostAddr.getPort())
            .setGpid(GlobalProcessId.ID)
            .addAllGroup(groupTags)
            .putAllAttrs(attrs)
            .setAnnouncerId(id())
            .setAnnouncedTS(HLC.INST.get())
            .build(), this, disposables);
    }

    @Override
    public CompletableFuture<Void> setServerGroups(String serverId, Set<String> groupTags) {
        Preconditions.checkState(!closed.get());
        Optional<RPCServer> announced = announcedServer(serverId);
        if (announced.isPresent()) {
            if (!groupTags.equals(Sets.newHashSet(announced.get().getGroupList()))) {
                RPCServer updated = announced.get().toBuilder()
                    .clearGroup()
                    .addAllGroup(groupTags)
                    .setAnnouncedTS(HLC.INST.get())
                    .build();
                return announce(updated);
            } else {
                return CompletableFuture.completedFuture(null);
            }
        }
        return CompletableFuture.failedFuture(new RuntimeException("Server not found: " + serverId));
    }

    @Override
    public CompletableFuture<Void> setTrafficRules(String tenantIdPrefix, Map<String, Integer> loadAssignment) {
        Preconditions.checkState(!closed.get());
        return setTrafficRule(tenantIdPrefix, loadAssignment);
    }

    @Override
    public CompletableFuture<Void> unsetTrafficRules(String tenantIdPrefix) {
        Preconditions.checkState(!closed.get());
        return unsetTrafficRule(tenantIdPrefix);
    }

    void close() {
        if (closed.compareAndSet(false, true)) {
            serverEndpointSubject.onComplete();
            disposables.dispose();
            super.destroy();
        }
    }

    private Set<ServerEndpoint> refreshAliveServerList(Map<String, RPCServer> announcedServers,
                                                       Set<ByteString> aliveAnnouncers) {
        Set<ServerEndpoint> aliveServers = Sets.newHashSet();
        for (RPCServer server : announcedServers.values()) {
            if (aliveAnnouncers.contains(server.getAnnouncerId())) {
                aliveServers.add(build(server));
            } else {
                // this is a side effect: revoke the announcement made by dead announcer
                log.debug("Remove not alive server announcement: {}", server.getId());
                revoke(server.getId());
            }
        }
        return aliveServers;
    }

    private ServerEndpoint build(RPCServer server) {
        return new ServerEndpoint(server.getAgentHostId(),
            server.getId(),
            server.getHost(),
            server.getPort(),
            GlobalProcessId.ID.equals(server.getGpid())
                ? new InProcessSocketAddress(server.getId())
                : new InetSocketAddress(server.getHost(), server.getPort()),
            Sets.newHashSet(server.getGroupList()),
            server.getAttrsMap(),
            GlobalProcessId.ID.equals(server.getGpid()));
    }

    private static class ServerRegistration implements IServerRegistration {

        private final RPCServiceTrafficManager manager;
        private final AtomicReference<RPCServer> localServer;
        private final Disposable disposable;
        private final CompositeDisposable disposables;

        private ServerRegistration(RPCServer server, RPCServiceTrafficManager announcer,
                                   CompositeDisposable disposables) {
            this.localServer = new AtomicReference<>(server);
            this.manager = announcer;
            this.disposables = disposables;

            // make an announcement via rpcServiceCRDT
            log.debug("Announce local server[{}]:{}", announcer.serviceUniqueName, server);
            announcer.announce(localServer.get()).join();

            // enforce the announcement consistent eventually
            disposable = announcer.announcedServers()
                .doOnDispose(() -> manager.revoke(localServer.get().getId()).join())
                .subscribe(serverMap -> {
                    RPCServer localServer = this.localServer.get();
                    if (!serverMap.containsKey(localServer.getId())) {
                        RPCServer toUpdate = localServer.toBuilder().setAnnouncedTS(HLC.INST.get()).build();
                        log.debug("Re-announce local server: {}", toUpdate);
                        // refresh announcement time
                        announcer.announce(toUpdate);
                    } else if (localServer.getAnnouncedTS() < serverMap.get(localServer.getId()).getAnnouncedTS()) {
                        localServer = serverMap.get(localServer.getId());
                        log.debug("Update local server from announcement: server={}", localServer);
                    }
                });
            disposables.add(disposable);
        }

        @Override
        public void stop() {
            disposables.remove(disposable);
            disposable.dispose();
        }
    }
}
