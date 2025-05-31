/*
 * Copyright (c) 2024. The BifroMQ Authors. All Rights Reserved.
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

import com.google.protobuf.ByteString;
import java.net.SocketAddress;
import java.util.Map;
import java.util.Set;

/**
 * The server endpoint record.
 *
 * @param hostId    the id of the host that the server is running on
 * @param id        the id of the server
 * @param address   the address of the server
 * @param port      the port of the server
 * @param hostAddr  the resolved host address of the server
 * @param groupTags the group tags of the server
 * @param attrs     the attributes of the server
 * @param inProc    whether the server is running in current process
 */
public record ServerEndpoint(ByteString hostId,
                             String id,
                             String address,
                             int port,
                             SocketAddress hostAddr,
                             Set<String> groupTags,
                             Map<String, String> attrs,
                             boolean inProc) {
}
