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

package org.apache.bifromq.basekv.raft.functest;

import org.apache.bifromq.basekv.raft.IRaftNode;
import org.apache.bifromq.basekv.raft.proto.RaftMessage;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

/**
 * Network testing utility for various network conditions simulation, e.g. delay, out-of-order, duplication and drop
 */
@Slf4j
public final class RaftNodeNetwork {
    private static final ConcurrentMap<String, BlockingQueue<NetworkPacket>> EMPTY_PEER
        = new ConcurrentHashMap<>();
    private static final BlockingQueue EMPTY_BUFFER = new PriorityBlockingQueue();

    private static class NetworkPacket {
        long sendAt;
        long priority;
        RaftMessage raftMessage;

        public NetworkPacket(long sendAt, long priority, RaftMessage raftMessage) {
            this.sendAt = sendAt;
            this.priority = priority;
            this.raftMessage = raftMessage;
        }
    }

    private ConcurrentHashMap<String, IRaftNode> raftNodes = new ConcurrentHashMap();
    private ConcurrentHashMap<String, IRaftNode.IRaftMessageSender> messageListeners = new ConcurrentHashMap();
    private ConcurrentHashMap<String, ConcurrentMap<String, BlockingQueue<NetworkPacket>>> links
        = new ConcurrentHashMap();
    private ConcurrentHashMap<String, ConcurrentMap<String, Float>> dropSettings = new ConcurrentHashMap();
    private ConcurrentHashMap<String, ConcurrentMap<String, Float>> duplicateSettings = new ConcurrentHashMap();
    private ConcurrentHashMap<String, ConcurrentMap<String, Float>> reorderSettings = new ConcurrentHashMap();
    private ConcurrentHashMap<String, ConcurrentMap<String, Integer>> delaySettings = new ConcurrentHashMap();
    private ConcurrentHashMap<String, ConcurrentMap<String, RaftMessage.MessageTypeCase>> ignoreSettings =
        new ConcurrentHashMap<>();

    private final ExecutorService tickExecutor =
        Executors.newSingleThreadExecutor(runnable -> new Thread(runnable, "RAFT-NETWORK"));

    private volatile long ticks;

    public void tick() {
        Future future = tickExecutor.submit(() -> {
            ticks++;
            for (String from : links.keySet()) {
                for (String to : links.get(from).keySet()) {
                    BlockingQueue<NetworkPacket> channel = links
                        .getOrDefault(from, EMPTY_PEER).getOrDefault(to, EMPTY_BUFFER);
                    while (!channel.isEmpty()) {
                        NetworkPacket networkPacket = channel.peek();
                        if (networkPacket.sendAt < ticks) {
                            IRaftNode peer = raftNodes.get(to);
                            if (peer != null && peer.isStarted()) {
                                log.trace("Send message: from={}, to={}, msg={}", from, to, networkPacket.raftMessage);
                                peer.receive(from, networkPacket.raftMessage);
                            }
                            channel.poll();
                        } else {
                            break;
                        }
                    }
                }
            }
        });
        try {
            future.get();
        } catch (Exception e) {
            log.error("Unexpected error", e);
        }
    }

    public void shutdown() {
        log.info("Shutting down raft network");
        try {
            log.info("Stopping network ticker");
            tickExecutor.shutdown();
            tickExecutor.awaitTermination(5, TimeUnit.SECONDS);
            new HashSet<>(raftNodes.keySet()).forEach(this::disconnect);
        } catch (InterruptedException e) {
            log.error("Shutdown with exception", e);
        }
    }

    public IRaftNode.IRaftMessageSender connect(IRaftNode raftNode) {
        raftNodes.putIfAbsent(raftNode.id(), raftNode);
        // build links
        ConcurrentMap<String, BlockingQueue<NetworkPacket>> tos =
            links.computeIfAbsent(raftNode.id(), key -> new ConcurrentHashMap<>());
        for (String key : links.keySet()) {
            if (!key.equals(raftNode.id())) {
                links.get(key).computeIfAbsent(raftNode.id(), k ->
                    new PriorityBlockingQueue<>(1024, Comparator.<NetworkPacket>comparingLong(p -> p.priority)));
                tos.computeIfAbsent(key, k ->
                    new PriorityBlockingQueue(1024, Comparator.<NetworkPacket>comparingLong(p -> p.priority)));
            }
        }
        return messageListeners.computeIfAbsent(raftNode.id(), id -> messages -> {
            ConcurrentMap<String, BlockingQueue<NetworkPacket>> peerChannels = links.get(id);
            if (peerChannels != null) {
                for (String to : messages.keySet()) {
                    if (!peerChannels.containsKey(to)) {
                        log.trace("Peer channel doesn't exists: from={}, to={}", id, to);
                        continue;
                    }
                    BlockingQueue<NetworkPacket> channel = peerChannels.get(to);
                    List<RaftMessage> peerMsgs = messages.get(to);
                    long sendAt = ticks + delay(id, to);
                    for (RaftMessage raftMessage : peerMsgs) {
                        if (needIgnore(id, to, raftMessage.getMessageTypeCase())) {
                            continue;
                        }
                        if (drop(id, to)) {
                            log.trace("Dropped message: from={}, to={}, msg={}", id, to, raftMessage);
                            continue;
                        }
                        long priority = System.nanoTime() + reorder(id, to);
                        log.trace("Buffered message: from={}, to={}, msg={}", id, to, raftMessage);
                        channel.add(new NetworkPacket(sendAt, priority, raftMessage));
                        if (duplicate(id, to)) {
                            log.trace("Buffered message: from={}, to={}, msg={}", id, to, raftMessage);
                            channel.offer(
                                new NetworkPacket(sendAt + delay(id, to), priority + reorder(id, to), raftMessage));
                        }
                    }
                }
            }
        });
    }

    public void disconnect(String id) {
        log.info("Disconnect raft node: id={}", id);
        raftNodes.get(id).stop().join();
        raftNodes.remove(id);
        messageListeners.remove(id);

        links.remove(id);
        links.values().forEach(tos -> tos.remove(id));

        dropSettings.remove(id);
        dropSettings.values().forEach(tos -> tos.remove(id));

        reorderSettings.remove(id);
        reorderSettings.values().forEach(tos -> tos.remove(id));

        duplicateSettings.remove(id);
        duplicateSettings.values().forEach(tos -> tos.remove(id));

        delaySettings.remove(id);
        delaySettings.values().forEach(tos -> tos.remove(id));
    }

    public void drop(String from, String to, float percent) {
        validate(from, to);
        dropSettings.compute(from, (f, v) -> {
            if (v == null) {
                v = new ConcurrentHashMap<>();
            }
            v.put(to, percent);
            return v;
        });
    }

    boolean drop(String from, String to) {
        validate(from, to);
        if (!dropSettings.containsKey(from)) {
            return false;
        }
        if (!dropSettings.get(from).containsKey(to)) {
            return false;
        }
        return ThreadLocalRandom.current().nextFloat() < dropSettings.get(from).get(to);
    }

    public void isolate(String peer) {
        validate(peer);
        for (String from : raftNodes.keySet()) {
            for (String to : raftNodes.keySet()) {
                if ((from.equals(peer) || to.equals(peer)) && !from.equals(to)) {
                    drop(from, to, 1.0f);
                }
            }
        }
    }

    public void integrate(String peer) {
        validate(peer);
        for (String from : raftNodes.keySet()) {
            for (String to : raftNodes.keySet()) {
                if ((from.equals(peer) || to.equals(peer)) && !from.equals(to)) {
                    drop(from, to, 0.0f);
                }
            }
        }
    }

    public void cut(String from, String to) {
        validate(from, to);
        drop(from, to, 1.0f);
        drop(to, from, 1.0f);
    }

    public void ignore(String from, String to, RaftMessage.MessageTypeCase messageTypeCase) {
        validate(from, to);
        ignoreSettings.compute(from, (f, v) -> {
            if (v == null) {
                v = new ConcurrentHashMap<>();
            }
            v.put(to, messageTypeCase);
            return v;
        });
    }

    boolean needIgnore(String from, String to, RaftMessage.MessageTypeCase messageTypeCase) {
        validate(from, to);
        if (!ignoreSettings.containsKey(from)) {
            return false;
        }
        if (!ignoreSettings.get(from).containsKey(to)) {
            return false;
        }
        return ignoreSettings.get(from).get(to) == messageTypeCase;
    }

    public void duplicate(String from, String to, float percent) {
        validate(from, to);
        duplicateSettings.compute(from, (f, v) -> {
            if (v == null) {
                v = new ConcurrentHashMap<>();
            }
            v.put(to, percent);
            return v;
        });
    }

    boolean duplicate(String from, String to) {
        validate(from, to);
        if (!duplicateSettings.containsKey(from)) {
            return false;
        }
        if (!duplicateSettings.get(from).containsKey(to)) {
            return false;
        }
        return ThreadLocalRandom.current().nextFloat() < duplicateSettings.get(from).get(to);
    }

    public void reorder(String from, String to, float percent) {
        validate(from, to);
        reorderSettings.compute(from, (f, v) -> {
            if (v == null) {
                v = new ConcurrentHashMap<>();
            }
            v.put(to, percent);
            return v;
        });
    }

    int reorder(String from, String to) {
        validate(from, to);
        if (!reorderSettings.containsKey(from)) {
            return 0;
        }
        if (!reorderSettings.get(from).containsKey(to)) {
            return 0;
        }
        return ThreadLocalRandom.current().nextFloat() < reorderSettings.get(from).get(to) ?
            ThreadLocalRandom.current().nextInt(-100000, 100000) : 0;
    }

    public void delay(String from, String to, int maxDelayTick) {
        validate(from, to);
        delaySettings.compute(from, (f, v) -> {
            if (v == null) {
                v = new ConcurrentHashMap<>();
            }
            v.put(to, maxDelayTick);
            return v;
        });
    }

    int delay(String from, String to) {
        validate(from, to);
        if (!delaySettings.containsKey(from)) {
            return 0;
        }
        if (!delaySettings.get(from).containsKey(to)) {
            return 0;
        }
        return ThreadLocalRandom.current().nextInt(1, delaySettings.get(from).get(to));
    }

    public void recover() {
        dropSettings.clear();
        duplicateSettings.clear();
        reorderSettings.clear();
        delaySettings.clear();
    }

    private void validate(String... nodeIds) {
        for (String nodeId : nodeIds) {
            if (!raftNodes.containsKey(nodeId)) {
                throw new IllegalStateException("node not found");
            }
        }
    }
}
