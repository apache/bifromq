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

package org.apache.bifromq.basekv.raft;

import org.apache.bifromq.basekv.raft.proto.RaftNodeSyncState;
import org.slf4j.Logger;

final class PeerLogReplicator implements IPeerLogReplicator {
    private PeerLogReplicatorState state;

    PeerLogReplicator(String peerId, RaftConfig config, IRaftStateStore stateStorage, Logger logger) {
        this.state = new PeerLogReplicatorStateProbing(peerId, config, stateStorage, logger);
    }

    @Override
    public long matchIndex() {
        return this.state.matchIndex();
    }

    @Override
    public long nextIndex() {
        return this.state.nextIndex();
    }

    @Override
    public RaftNodeSyncState status() {
        return this.state.state();
    }

    @Override
    public boolean tick() {
        PeerLogReplicatorState newState = this.state.tick();
        if (this.state != newState) {
            this.state = newState;
            return true;
        }
        return false;
    }

    @Override
    public long catchupRate() {
        return this.state.catchupRate();
    }

    @Override
    public boolean pauseReplicating() {
        return this.state.pauseReplicating();
    }

    @Override
    public boolean needHeartbeat() {
        return this.state.needHeartbeat();
    }

    @Override
    public boolean backoff(long peerRejectedIndex, long peerLastIndex) {
        PeerLogReplicatorState newState = this.state.backoff(peerRejectedIndex, peerLastIndex);
        if (this.state != newState) {
            this.state = newState;
            return true;
        }
        return false;
    }

    @Override
    public boolean confirmMatch(long peerLastIndex) {
        PeerLogReplicatorState newState = this.state.confirmMatch(peerLastIndex);
        if (this.state != newState) {
            this.state = newState;
            return true;
        }
        return false;
    }

    @Override
    public boolean replicateBy(long endIndex) {
        PeerLogReplicatorState newState = this.state.replicateTo(endIndex);
        if (this.state != newState) {
            this.state = newState;
            return true;
        }
        return false;
    }
}
