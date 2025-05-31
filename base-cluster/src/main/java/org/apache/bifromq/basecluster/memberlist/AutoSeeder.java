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

package org.apache.bifromq.basecluster.memberlist;

import org.apache.bifromq.basecluster.membership.proto.HostEndpoint;
import org.apache.bifromq.basecluster.membership.proto.Join;
import org.apache.bifromq.basecluster.messenger.IMessenger;
import org.apache.bifromq.basecluster.proto.ClusterMessage;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.google.common.collect.Sets;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Metrics;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
import io.reactivex.rxjava3.disposables.Disposable;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class AutoSeeder {
    private final IMessenger messenger;
    private final Scheduler scheduler;
    private final IHostMemberList memberList;
    private final IHostAddressResolver addressResolver;
    private final Duration joinInterval;
    private final LoadingCache<InetSocketAddress, CompletableFuture<Void>> joiningSeeds;
    private final AtomicBoolean stopped = new AtomicBoolean();
    private final AtomicBoolean scheduled = new AtomicBoolean();
    private final CompositeDisposable disposables = new CompositeDisposable();
    private final Gauge seedNumGauge;
    private volatile Set<HostEndpoint> aliveMembers = new HashSet<>();
    private volatile Disposable job;

    public AutoSeeder(IMessenger messenger,
                      Scheduler scheduler,
                      IHostMemberList memberList,
                      IHostAddressResolver addressResolver,
                      Duration joinTimeout,
                      Duration joinInterval,
                      String... tags) {
        this.messenger = messenger;
        this.scheduler = scheduler;
        this.memberList = memberList;
        this.addressResolver = addressResolver;
        this.joinInterval = joinInterval;
        joiningSeeds = Caffeine.newBuilder()
            .maximumSize(30)
            .expireAfterWrite(joinTimeout)
            .removalListener((RemovalListener<InetSocketAddress, CompletableFuture<Void>>) (key, value, cause) -> {
                if (value == null) {
                    return;
                }
                if (cause.wasEvicted()) {
                    log.debug("Stop trying to join seed address[{}]", key);
                    value.completeExceptionally(new UnknownHostException(key + " is unreachable"));
                } else {
                    if (stopped.get()) {
                        log.debug("Abort joining seed address[{}]", key);
                        value.completeExceptionally(new IllegalStateException("Seeding has stopped"));
                    } else {
                        log.debug("Join seed address[{}] success", key);
                        value.complete(null);
                    }
                }
            })
            .build(k -> new CompletableFuture<>());
        disposables.add(memberList.members().observeOn(scheduler).subscribe(m -> this.clearJoined(m.keySet())));
        disposables.add(memberList.members()
            .observeOn(scheduler)
            .subscribe(members -> aliveMembers = members.keySet()));
        seedNumGauge = Gauge.builder("basecluster.seed.num", joiningSeeds::estimatedSize)
            .tags(tags)
            .register(Metrics.globalRegistry);

    }

    public CompletableFuture<Void> join(Set<InetSocketAddress> seeds) {
        if (stopped.get()) {
            return CompletableFuture.failedFuture(new IllegalStateException("Seeder has stopped"));
        }
        Set<InetSocketAddress> known = new HashSet<>();
        for (HostEndpoint endpoint : aliveMembers) {
            InetSocketAddress addr = addressResolver.resolve(endpoint);
            if (seeds.contains(addr)) {
                known.add(addr);
            }
        }
        Set<InetSocketAddress> newSeeds = Sets.difference(seeds, known);
        CompletableFuture<?>[] joinFutures = joiningSeeds.getAll(newSeeds)
            .values()
            .toArray(CompletableFuture[]::new);
        schedule(0);
        return CompletableFuture.allOf(joinFutures);
    }

    public void stop() {
        if (stopped.compareAndSet(false, true)) {
            joiningSeeds.invalidateAll();
            disposables.dispose();
            if (job != null) {
                job.dispose();
            }
            Metrics.globalRegistry.remove(seedNumGauge);
        }
    }

    private void schedule(long delayInMS) {
        if (!stopped.get() && scheduled.compareAndSet(false, true)) {
            job = scheduler.scheduleDirect(this::run, delayInMS, TimeUnit.MILLISECONDS);
        }
    }

    private Set<InetSocketAddress> clearJoined(Set<HostEndpoint> endpoints) {
        Set<InetSocketAddress> knownAddresses = new HashSet<>();
        Set<InetSocketAddress> allJoiningSeeds = Sets.newHashSet(joiningSeeds.asMap().keySet());
        endpoints.forEach(endpoint -> knownAddresses.add(addressResolver.resolve(endpoint)));
        joiningSeeds.invalidateAll(Sets.intersection(knownAddresses, allJoiningSeeds));
        // run a compaction so that expired entries could be cleanup as soon as possible
        joiningSeeds.cleanUp();
        return Sets.difference(allJoiningSeeds, knownAddresses);
    }

    private void run() {
        Set<InetSocketAddress> toJoinSeeds = clearJoined(aliveMembers);
        for (InetSocketAddress seedAddr : toJoinSeeds) {
            if (seedAddr.isUnresolved()) {
                // try resolve
                seedAddr = new InetSocketAddress(seedAddr.getHostName(), seedAddr.getPort());
                log.debug("Resolving hostname[{}] to {}",
                    seedAddr.getHostName(), seedAddr.getAddress().getHostAddress());
                if (seedAddr.isUnresolved()) {
                    continue;
                }
            }
            final InetSocketAddress finalSeedAddr = seedAddr;
            log.debug("Send join message to address[{}]", finalSeedAddr);
            messenger.send(ClusterMessage.newBuilder()
                    .setJoin(Join.newBuilder()
                        .setMember(memberList.local())
                        .build())
                    .build(), finalSeedAddr, true)
                .whenComplete((v, e) -> {
                    if (e != null) {
                        log.warn("failed to send join message to {}, due to {}", finalSeedAddr, e.getMessage());
                    }
                });
        }
        scheduled.set(false);
        if (stopped.get()) {
            joiningSeeds.invalidateAll();
        } else if (!joiningSeeds.asMap().isEmpty()) {
            schedule(joinInterval.toMillis());
        }
    }
}
