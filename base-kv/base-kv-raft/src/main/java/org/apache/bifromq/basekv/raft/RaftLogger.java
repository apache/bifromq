/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.bifromq.basekv.raft;

import java.util.HashMap;
import java.util.Map;
import org.apache.bifromq.basekv.raft.proto.ClusterConfig;
import org.apache.bifromq.logger.MDCLogger;

class RaftLogger extends MDCLogger {
    private static final String MDC_KEY_ID = "id";
    private static final String MDC_KEY_STATE = "state";
    private static final String MDC_KEY_LEADER = "leader";
    private static final String MDC_KEY_TERM = "term";
    private static final String MDC_KEY_FIRST = "first";
    private static final String MDC_KEY_LAST = "last";
    private static final String MDC_KEY_COMMIT = "commit";
    private static final String MDC_KEY_CONFIG = "config";
    private final IRaftNodeState state;
    private final ThreadLocal<Map<String, String>> extraContext = ThreadLocal.withInitial(HashMap::new);

    protected RaftLogger(IRaftNodeState state, String... tags) {
        super(state.getClass().getName(), tags);
        this.state = state;
    }

    @Override
    protected Map<String, String> extraContext() {
        Map<String, String> extraCtx = extraContext.get();
        extraCtx.put(MDC_KEY_ID, state.id());
        extraCtx.put(MDC_KEY_STATE, state.getState().name());
        extraCtx.put(MDC_KEY_LEADER, state.currentLeader());
        extraCtx.put(MDC_KEY_TERM, Long.toUnsignedString(state.currentTerm()));
        extraCtx.put(MDC_KEY_FIRST, Long.toUnsignedString(state.firstIndex()));
        extraCtx.put(MDC_KEY_LAST, Long.toUnsignedString(state.lastIndex()));
        extraCtx.put(MDC_KEY_COMMIT, Long.toUnsignedString(state.commitIndex()));
        extraCtx.put(MDC_KEY_CONFIG, printClusterConfig(state.latestClusterConfig()));
        return extraCtx;
    }

    private String printClusterConfig(ClusterConfig clusterConfig) {
        return String.format("[c:%s,v:%s,l:%s,nv:%s,nl:%s]",
            clusterConfig.getCorrelateId(),
            clusterConfig.getVotersList(),
            clusterConfig.getLearnersList(),
            clusterConfig.getNextVotersList(),
            clusterConfig.getNextLearnersList());
    }
}
