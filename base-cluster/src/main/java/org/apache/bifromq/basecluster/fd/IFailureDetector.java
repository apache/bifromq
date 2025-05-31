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

package org.apache.bifromq.basecluster.fd;

import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.schedulers.Timed;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

public interface IFailureDetector {
    Duration baseProbeInterval();

    Duration baseProbeTimeout();

    /**
     * Start the failure detector using a specified member selector
     *
     * @param targetSelector
     */
    void start(IProbingTargetSelector targetSelector);

    /**
     * Stop the failure detector
     */
    CompletableFuture<Void> shutdown();

    /**
     * Hot observable of probe-succeed target
     *
     * @return
     */
    Observable<Timed<IProbingTarget>> succeeding();

    /**
     * Hot observable of probe-fail target
     *
     * @return
     */
    Observable<Timed<IProbingTarget>> suspecting();

    void penaltyHealth();

    /**
     * Hot observable of scoring the local health based on failure detecting process
     *
     * @return
     */
    Observable<Integer> healthScoring();
}
