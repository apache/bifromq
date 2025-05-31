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

package org.apache.bifromq.basekv.raft.functest.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD})
public @interface Config {
    boolean asyncAppend() default true;

    boolean disableForwardProposal() default false;

    int electionTimeoutTick() default 10;

    int heartbeatTimeoutTick() default 1;

    int installSnapshotTimeoutTick() default 20;

    int maxInflightAppends() default 10;

    int maxSizePerAppend() default 1024;

    int maxUnappliedEntries() default 10;

    boolean preVote() default true;

    int readOnlyBatch() default 10;

    boolean readOnlyLeaderLeaseMode() default true;
}
