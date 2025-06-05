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

package org.apache.bifromq.baserpc;

import org.apache.bifromq.baserpc.metrics.IRPCMeter;
import io.grpc.Context;
import java.util.Map;

public class RPCContext {
    public static final Context.Key<String> TENANT_ID_CTX_KEY = Context.key("TenantId");
    public static final Context.Key<IRPCMeter.IRPCMethodMeter> METER_KEY_CTX_KEY = Context.key("MeterKey");
    public static final Context.Key<String> DESIRED_SERVER_ID_CTX_KEY = Context.key("DesiredServerId");
    public static final Context.Key<Map<String, String>> CUSTOM_METADATA_CTX_KEY = Context.key("CustomMetadata");
}
