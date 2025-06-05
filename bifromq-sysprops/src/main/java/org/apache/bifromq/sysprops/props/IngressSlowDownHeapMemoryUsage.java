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

package org.apache.bifromq.sysprops.props;

import org.apache.bifromq.sysprops.BifroMQSysProp;
import org.apache.bifromq.sysprops.parser.DoubleParser;

/**
 * * The system property used to configure the heap memory usage threshold for slowing down the ingress mqtt traffic.
 */
public class IngressSlowDownHeapMemoryUsage extends BifroMQSysProp<Double, DoubleParser> {
    public static final IngressSlowDownHeapMemoryUsage INSTANCE = new IngressSlowDownHeapMemoryUsage();

    private IngressSlowDownHeapMemoryUsage() {
        super("ingress_slowdown_heap_memory_usage", 0.9,
            DoubleParser.from(0.1, 1.0, false));
    }
}
