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

package org.apache.bifromq.plugin.settingprovider;

import org.pf4j.ExtensionPoint;

public interface ISettingProvider extends ExtensionPoint {
    /**
     * Provide a value of the setting for given tenant. The method will be called by BifroMQ working thread, so it's
     * expected to be performant and non-blocking otherwise the overall performance will be greatly impacted. It's
     * allowed to return null to reuse the current setting value, in case the value could not be determined in timely
     * manner.
     *
     * @param setting  the setting for the client
     * @param tenantId the id of the calling tenant
     * @return the setting value for the client or null
     */
    <R> R provide(Setting setting, String tenantId);

    /**
     * This method will be called during broker shutdown.
     */
    default void close() {

    }
}
