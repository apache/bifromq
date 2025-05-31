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

package org.apache.bifromq.inbox.server;

import static org.apache.bifromq.base.util.CompletableFutureUtil.unwrap;
import static org.apache.bifromq.inbox.util.PipelineUtil.PIPELINE_ATTR_KEY_DELIVERERKEY;
import static org.apache.bifromq.inbox.util.PipelineUtil.PIPELINE_ATTR_KEY_ID;

import org.apache.bifromq.baserpc.server.AckStream;
import org.apache.bifromq.basescheduler.exception.BackPressureException;
import org.apache.bifromq.basescheduler.exception.BatcherUnavailableException;
import org.apache.bifromq.inbox.rpc.proto.InboxFetchHint;
import org.apache.bifromq.inbox.rpc.proto.InboxFetched;
import org.apache.bifromq.inbox.server.scheduler.FetchRequest;
import org.apache.bifromq.inbox.storage.proto.BatchFetchRequest;
import org.apache.bifromq.inbox.storage.proto.Fetched;
import io.grpc.stub.StreamObserver;
import io.reactivex.rxjava3.disposables.Disposable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@Slf4j
final class InboxFetchPipeline extends AckStream<InboxFetchHint, InboxFetched> implements IInboxFetcher {
    private static final int NOT_KNOWN_CAPACITY = -1;
    private final String id;
    private final String delivererKey;
    private final Map<Long, FetchState> inboxFetchSessions;
    private final Map<InboxId, Set<Long>> inboxSessionMap = new ConcurrentHashMap<>();
    private final Fetcher fetcher;
    private final Disposable disposable;
    private volatile boolean closed = false;

    public InboxFetchPipeline(StreamObserver<InboxFetched> responseObserver,
                              Fetcher fetcher,
                              InboxFetcherRegistry registry) {
        super(responseObserver);
        this.id = metadata(PIPELINE_ATTR_KEY_ID);
        this.delivererKey = metadata(PIPELINE_ATTR_KEY_DELIVERERKEY);

        inboxFetchSessions = new ConcurrentHashMap<>();
        this.fetcher = fetcher;
        registry.reg(this);
        disposable = ack()
            .doFinally(() -> {
                registry.unreg(this);
                closed = true;
            })
            .subscribe(fetchHint -> {
                String inboxId = fetchHint.getInboxId();
                log.trace("Got hint: tenantId={}, inboxId={}\n{}", tenantId, inboxId, fetchHint);
                if (fetchHint.getCapacity() < 0) {
                    inboxFetchSessions.computeIfPresent(fetchHint.getSessionId(), (k, v) -> {
                        inboxSessionMap.remove(new InboxId(v.inboxId, v.incarnation));
                        return null;
                    });
                } else {
                    FetchState fetchState = inboxFetchSessions.compute(fetchHint.getSessionId(), (k, v) -> {
                        if (v == null) {
                            v = new FetchState(fetchHint.getInboxId(),
                                fetchHint.getIncarnation(),
                                fetchHint.getSessionId());
                            inboxSessionMap.computeIfAbsent(
                                new InboxId(fetchHint.getInboxId(), fetchHint.getIncarnation()),
                                k1 -> new HashSet<>()).add(fetchHint.getSessionId());
                        }
                        v.lastFetchQoS0Seq.set(fetchHint.getLastFetchQoS0Seq());
                        v.lastFetchSendBufferSeq.set(fetchHint.getLastFetchSendBufferSeq());
                        v.downStreamCapacity.set(Math.max(0, fetchHint.getCapacity()));
                        return v;
                    });
                    log.trace("Fetch state update: tenantId={}, inbox={}\n{}", tenantId, inboxId, fetchState);
                    fetch(fetchState);
                }
            });
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public String tenantId() {
        return tenantId;
    }

    @Override
    public String delivererKey() {
        return delivererKey;
    }

    @Override
    public void send(InboxFetched message) {
        synchronized (this) {
            super.send(message);
        }
    }

    @Override
    public boolean signalFetch(String inboxId, long incarnation) {
        log.trace("Signal fetch: tenantId={}, inboxId={}", tenantId, inboxId);
        // signal fetch won't refresh expiry
        Set<Long> sessionIds = inboxSessionMap.getOrDefault(new InboxId(inboxId, incarnation), Collections.emptySet());
        for (Long sessionId : sessionIds) {
            FetchState fetchState = inboxFetchSessions.get(sessionId);
            if (fetchState != null) {
                fetchState.hasMore.set(true);
                fetchState.signalFetchTS.set(System.nanoTime());
                fetch(fetchState);
            }
        }
        return !sessionIds.isEmpty();
    }

    @Override
    public void close() {
        super.close();
        disposable.dispose();
    }

    private void fetch(long sessionId) {
        FetchState fetchState = inboxFetchSessions.get(sessionId);
        if (fetchState != null) {
            fetch(fetchState);
        }
    }

    private void fetch(FetchState fetchState) {
        if (closed) {
            return;
        }
        if (fetchState.fetching.compareAndSet(false, true)) {
            long sessionId = fetchState.sessionId;
            String inboxId = fetchState.inboxId;
            long incarnation = fetchState.incarnation;
            log.trace("Fetching inbox: tenantId={}, inboxId={}", tenantId, inboxId);
            FetchRequest request = new FetchRequest(tenantId, inboxId, incarnation,
                BatchFetchRequest.Params.newBuilder()
                    .setMaxFetch(fetchState.downStreamCapacity.get())
                    .setQos0StartAfter(fetchState.lastFetchQoS0Seq.get())
                    .setSendBufferStartAfter(fetchState.lastFetchSendBufferSeq.get())
                    .build());
            long fetchTS = System.nanoTime();
            fetcher.fetch(request).whenComplete(unwrap((fetched, e) -> {
                if (closed) {
                    return;
                }
                if (e != null) {
                    try {
                        inboxFetchSessions.computeIfPresent(sessionId, (k, v) -> {
                            inboxSessionMap.remove(new InboxId(v.inboxId, v.incarnation));
                            return null;
                        });
                        if (e instanceof BatcherUnavailableException) {
                            send(InboxFetched.newBuilder()
                                .setSessionId(fetchState.sessionId)
                                .setInboxId(inboxId)
                                .setIncarnation(incarnation)
                                .setFetched(Fetched.newBuilder()
                                    .setResult(Fetched.Result.TRY_LATER)
                                    .build())
                                .build());
                            return;
                        }
                        if (e instanceof BackPressureException) {
                            send(InboxFetched.newBuilder()
                                .setSessionId(fetchState.sessionId)
                                .setInboxId(inboxId)
                                .setIncarnation(incarnation)
                                .setFetched(Fetched.newBuilder()
                                    .setResult(Fetched.Result.BACK_PRESSURE_REJECTED)
                                    .build())
                                .build());
                            return;
                        }
                        log.debug("Failed to fetch inbox: tenantId={}, inboxId={}, incarnation={}",
                            tenantId, inboxId, incarnation, e);
                        send(InboxFetched.newBuilder()
                            .setSessionId(fetchState.sessionId)
                            .setInboxId(inboxId)
                            .setIncarnation(incarnation)
                            .setFetched(Fetched.newBuilder()
                                .setResult(Fetched.Result.ERROR)
                                .build())
                            .build());

                    } catch (Throwable t) {
                        log.error("Unexpected error", t);
                    }
                } else {
                    log.trace("Fetched inbox: tenantId={}, inboxId={}, incarnation={}\n{}",
                        tenantId, inboxId, incarnation, fetched);
                    try {
                        send(InboxFetched.newBuilder()
                            .setSessionId(sessionId)
                            .setInboxId(inboxId)
                            .setIncarnation(incarnation)
                            .setFetched(fetched).build());
                        if (fetched.getQos0MsgCount() > 0 || fetched.getSendBufferMsgCount() > 0) {
                            if (fetched.getQos0MsgCount() > 0) {
                                fetchState.lastFetchQoS0Seq.set(
                                    fetched.getQos0Msg(fetched.getQos0MsgCount() - 1).getSeq());
                            }
                            int fetchedCount = 0;
                            if (fetched.getSendBufferMsgCount() > 0) {
                                fetchedCount += fetched.getSendBufferMsgCount();
                                fetchState.downStreamCapacity.accumulateAndGet(fetched.getSendBufferMsgCount(),
                                    (a, b) -> a == NOT_KNOWN_CAPACITY ? a : Math.max(a - b, 0));
                                fetchState.lastFetchSendBufferSeq.set(
                                    fetched.getSendBufferMsg(fetched.getSendBufferMsgCount() - 1).getSeq());
                            }
                            fetchState.hasMore.set(fetchedCount >= request.params().getMaxFetch()
                                || fetchState.signalFetchTS.get() > fetchTS);
                        } else {
                            fetchState.hasMore.set(fetchState.signalFetchTS.get() > fetchTS);
                        }
                        fetchState.fetching.set(false);
                        inboxFetchSessions.compute(sessionId, (k, v) -> {
                            if (v == null) {
                                inboxSessionMap.computeIfAbsent(new InboxId(fetchState.inboxId, fetchState.incarnation),
                                    k1 -> new HashSet<>()).add(fetchState.sessionId);
                            }
                            return fetchState;
                        });
                        if (fetchState.downStreamCapacity.get() > 0 && fetchState.hasMore.get()) {
                            fetch(sessionId);
                        }
                    } catch (Throwable t) {
                        log.error("Unexpected error", t);
                    }
                }
            }));
        }
    }

    interface Fetcher {
        CompletableFuture<Fetched> fetch(FetchRequest request);
    }

    private record InboxId(String inboxId, long incarnation) {
    }

    @ToString
    private static class FetchState {
        final String inboxId;
        final long incarnation;
        final long sessionId;
        final AtomicBoolean fetching = new AtomicBoolean(false);
        final AtomicBoolean hasMore = new AtomicBoolean(true);
        final AtomicLong signalFetchTS = new AtomicLong();
        final AtomicInteger downStreamCapacity = new AtomicInteger(NOT_KNOWN_CAPACITY);
        final AtomicLong lastFetchQoS0Seq = new AtomicLong(-1);
        final AtomicLong lastFetchSendBufferSeq = new AtomicLong(-1);

        FetchState(String inboxId, long incarnation, long sessionId) {
            this.inboxId = inboxId;
            this.incarnation = incarnation;
            this.sessionId = sessionId;
        }
    }
}
