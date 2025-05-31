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


import io.reactivex.rxjava3.core.Observable;
import java.util.Map;
import java.util.Set;

/**
 * The interface for accessing the landscape of a service.
 */
public interface IRPCServiceLandscape {
    /**
     * Current traffic rules of the service.
     *
     * @return an observable of traffic rules
     */
    Observable<Map<String, Map<String, Integer>>> trafficRules();

    /**
     * Get an observable of server endpoints of the service.
     *
     * @return an observable of server endpoints
     */
    Observable<Set<ServerEndpoint>> serverEndpoints();
}
