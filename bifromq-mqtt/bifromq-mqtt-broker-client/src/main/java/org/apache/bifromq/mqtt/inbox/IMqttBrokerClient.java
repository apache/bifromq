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

package org.apache.bifromq.mqtt.inbox;

import org.apache.bifromq.baserpc.client.IConnectable;
import org.apache.bifromq.plugin.subbroker.ISubBroker;
import java.util.concurrent.CompletableFuture;
import org.apache.bifromq.mqtt.inbox.rpc.proto.SubReply;
import org.apache.bifromq.mqtt.inbox.rpc.proto.UnsubReply;
import org.apache.bifromq.type.QoS;

public interface IMqttBrokerClient extends ISubBroker, IConnectable, AutoCloseable {
    static MqttBrokerClientBuilder newBuilder() {
        return new MqttBrokerClientBuilder();
    }

    CompletableFuture<SubReply> sub(long reqId,
                                    String tenantId,
                                    String sessionId,
                                    String topicFilter,
                                    QoS qos,
                                    String brokerServerId);

    CompletableFuture<UnsubReply> unsub(long reqId,
                                        String tenantId,
                                        String sessionId,
                                        String topicFilter,
                                        String brokerServerId);

    @Override
    default int id() {
        return 0;
    }

    void close();
}
