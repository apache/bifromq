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

package org.apache.bifromq.plugin.eventcollector;

import org.pf4j.ExtensionPoint;

public interface IEventCollector extends ExtensionPoint {
    /**
     * Implement this method to receive various events at runtime.
     * <p>
     * Note: To reduce memory pressure, the argument event object will be reused in later call, so the ownership is not
     * transferred to the method implementation. Make a clone if needed.
     *
     * @param event the event reported
     */
    void report(Event<?> event);

    /**
     * This method will be called during broker shutdown
     */
    default void close() {
    }
}
