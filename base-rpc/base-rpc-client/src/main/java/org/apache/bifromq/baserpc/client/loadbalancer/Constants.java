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

package org.apache.bifromq.baserpc.client.loadbalancer;

import io.grpc.Attributes;
import io.grpc.ConnectivityStateInfo;
import io.grpc.Metadata;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

public class Constants {
    public static final Attributes.Key<AtomicReference<ConnectivityStateInfo>> STATE_INFO =
        Attributes.Key.create("state-info");

    public static final Attributes.Key<Map<String, Map<String, Integer>>> TRAFFIC_DIRECTIVE_ATTR_KEY =
        Attributes.Key.create("TrafficDirective");
    public static final Attributes.Key<String> SERVER_ID_ATTR_KEY = Attributes.Key.create("ServerId");
    public static final Attributes.Key<Set<String>> SERVER_GROUP_TAG_ATTR_KEY = Attributes.Key.create("ServerGroupTag");
    public static final Attributes.Key<Boolean> IN_PROC_SERVER_ATTR_KEY = Attributes.Key.create("InProc");

    public static final Metadata.Key<String> DESIRED_SERVER_META_KEY =
        Metadata.Key.of("desired_server_id", Metadata.ASCII_STRING_MARSHALLER);
}
