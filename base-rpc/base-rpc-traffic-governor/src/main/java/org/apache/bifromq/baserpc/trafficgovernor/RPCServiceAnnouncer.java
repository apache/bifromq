/*
 * Copyright (c) 2023. The BifroMQ Authors. All Rights Reserved.
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
import static com.google.protobuf.ByteString.copyFromUtf8;
import static java.util.Collections.emptyMap;

import org.apache.bifromq.basecrdt.core.api.CRDTURI;
import org.apache.bifromq.basecrdt.core.api.CausalCRDTType;
import org.apache.bifromq.basecrdt.core.api.IMVReg;
import org.apache.bifromq.basecrdt.core.api.IORMap;
import org.apache.bifromq.basecrdt.core.api.MVRegOperation;
import org.apache.bifromq.basecrdt.core.api.ORMapOperation;
import org.apache.bifromq.basecrdt.proto.Replica;
import org.apache.bifromq.basecrdt.service.ICRDTService;
import org.apache.bifromq.basehlc.HLC;
import org.apache.bifromq.baserpc.proto.LoadAssignment;
import org.apache.bifromq.baserpc.proto.RPCServer;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
import io.reactivex.rxjava3.subjects.BehaviorSubject;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
abstract class RPCServiceAnnouncer {
    private static final ByteString SERVER_LIST_KEY = ByteString.copyFrom(new byte[] {0x00});
    private static final ByteString TRAFFIC_RULES_KEY = ByteString.copyFrom(new byte[] {0x01});

    protected final String serviceUniqueName;
    protected final ICRDTService crdtService;
    private final Replica crdtReplica;
    private final IORMap rpcServiceCRDT;
    private final BehaviorSubject<Map<String, RPCServer>> serverEndpointSubject;
    private final BehaviorSubject<Map<String, Map<String, Integer>>> trafficRulesSubject;
    private final CompositeDisposable disposable = new CompositeDisposable();

    protected RPCServiceAnnouncer(String serviceUniqueName, ICRDTService crdtService) {
        this.serviceUniqueName = serviceUniqueName;
        this.crdtService = crdtService;
        this.rpcServiceCRDT = crdtService.host(CRDTURI.toURI(CausalCRDTType.ormap, "RPC:" + serviceUniqueName));
        this.crdtReplica = rpcServiceCRDT.id();
        Map<String, RPCServer> serverMap = buildAnnouncedServers(HLC.INST.get());
        serverEndpointSubject =
            serverMap.isEmpty() ? BehaviorSubject.create() : BehaviorSubject.createDefault(serverMap);
        trafficRulesSubject =
            BehaviorSubject.createDefault(buildAnnouncedTrafficRules(HLC.INST.get()).orElse(emptyMap()));
        disposable.add(rpcServiceCRDT.getORMap(SERVER_LIST_KEY)
            .inflation()
            .observeOn(RPC_SHARED_SCHEDULER)
            .map(this::buildAnnouncedServers)
            .subscribe(serverEndpointSubject::onNext));
        disposable.add(rpcServiceCRDT.getORMap(TRAFFIC_RULES_KEY)
            .inflation()
            .observeOn(RPC_SHARED_SCHEDULER)
            .map(this::buildAnnouncedTrafficRules)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .subscribe(trafficRulesSubject::onNext));
    }

    protected ByteString id() {
        return crdtReplica.getId();
    }

    protected void destroy() {
        serverEndpointSubject.onComplete();
        trafficRulesSubject.onComplete();
        disposable.dispose();
        crdtService.stopHosting(rpcServiceCRDT.id().getUri()).join();
    }

    protected CompletableFuture<Void> announce(RPCServer server) {
        return rpcServiceCRDT.execute(ORMapOperation
            .update(SERVER_LIST_KEY, copyFromUtf8(server.getId()))
            .with(MVRegOperation.write(server.toByteString())));
    }

    protected CompletableFuture<Void> setTrafficRule(String tenantIdPrefix, Map<String, Integer> weightedGroups) {
        return rpcServiceCRDT.execute(ORMapOperation.update(TRAFFIC_RULES_KEY)
            .with(ORMapOperation.update(ByteString.copyFromUtf8(tenantIdPrefix))
                .with(MVRegOperation.write(LoadAssignment
                    .newBuilder()
                    .putAllWeightedGroup(weightedGroups)
                    .build()
                    .toByteString()))));
    }

    protected CompletableFuture<Void> unsetTrafficRule(String tenantIdPrefix) {
        return rpcServiceCRDT.execute(ORMapOperation.update(TRAFFIC_RULES_KEY)
            .with(ORMapOperation.remove(ByteString.copyFromUtf8(tenantIdPrefix))
                .of(CausalCRDTType.mvreg)));
    }

    protected CompletableFuture<Void> revoke(String id) {
        return rpcServiceCRDT.execute(ORMapOperation
            .remove(SERVER_LIST_KEY, copyFromUtf8(id))
            .of(CausalCRDTType.mvreg));
    }

    protected Optional<RPCServer> announcedServer(String id) {
        return announcedServer(rpcServiceCRDT.getMVReg(SERVER_LIST_KEY, copyFromUtf8(id)));
    }

    private Optional<RPCServer> announcedServer(IMVReg mvReg) {
        RPCServer server = null;
        Iterator<ByteString> itr = mvReg.read();
        while (itr.hasNext()) {
            try {
                RPCServer s = RPCServer.parseFrom(itr.next());
                if (server == null) {
                    server = s;
                } else {
                    server = server.getAnnouncedTS() < s.getAnnouncedTS() ? s : server;
                }
            } catch (InvalidProtocolBufferException e) {
                log.error("Unable to parse RPCServer from crdt", e);
            }
        }
        return Optional.ofNullable(server);
    }

    protected Observable<Map<String, RPCServer>> announcedServers() {
        return serverEndpointSubject;
    }

    private Map<String, RPCServer> buildAnnouncedServers(long t) {
        IORMap serverListORMap = rpcServiceCRDT.getORMap(SERVER_LIST_KEY);
        Iterator<IORMap.ORMapKey> keyItr = serverListORMap.keys();
        Map<String, RPCServer> announced = Maps.newHashMap();
        while (keyItr.hasNext()) {
            IORMap.ORMapKey orMapKey = keyItr.next();
            assert orMapKey.valueType() == CausalCRDTType.mvreg;
            Optional<RPCServer> rpcServer = announcedServer(serverListORMap.getMVReg(orMapKey.key()));
            rpcServer.ifPresent(server -> announced.put(server.getId(), server));
        }
        log.debug("Build service[{}]'s server list at {}\n{}", serviceUniqueName, t, announced);
        return announced;
    }

    protected Observable<Map<String, Map<String, Integer>>> trafficRules() {
        return trafficRulesSubject;
    }

    protected Observable<Set<ByteString>> aliveAnnouncers() {
        return crdtService.aliveReplicas(crdtReplica.getUri())
            .map(r -> r.stream().map(Replica::getId).collect(Collectors.toSet()));
    }

    private Optional<Map<String, Map<String, Integer>>> buildAnnouncedTrafficRules(long t) {
        Map<String, Map<String, Integer>> trafficDirective = Maps.newHashMap();
        IORMap directives = rpcServiceCRDT.getORMap(TRAFFIC_RULES_KEY);
        directives.keys().forEachRemaining(key -> {
            assert key.valueType() == CausalCRDTType.mvreg;
            Optional<LoadAssignment> la = parseLoadAssignment(directives.getMVReg(key.key()));
            la.ifPresent(
                loadAssignment -> trafficDirective.put(key.key().toStringUtf8(), loadAssignment.getWeightedGroupMap()));
        });
        return Optional.of(trafficDirective);
    }

    private Optional<LoadAssignment> parseLoadAssignment(IMVReg mvReg) {
        LoadAssignment loadAssignment = null;
        Iterator<ByteString> itr = mvReg.read();
        while (itr.hasNext()) {
            try {
                LoadAssignment next = LoadAssignment.parseFrom(itr.next());
                if (loadAssignment == null) {
                    loadAssignment = next;
                } else {
                    loadAssignment = loadAssignment.getAnnouncedTS() < next.getAnnouncedTS() ? next : loadAssignment;
                }
            } catch (InvalidProtocolBufferException e) {
                log.error("Unable to parse LoadAssignment from crdt", e);
            }
        }
        return Optional.ofNullable(loadAssignment);
    }
}
