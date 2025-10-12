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

package org.apache.bifromq.starter.config.model.mqtt;

import com.fasterxml.jackson.annotation.JsonMerge;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.protobuf.Struct;
import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.apache.bifromq.baseenv.EnvProvider;
import org.apache.bifromq.starter.config.model.mqtt.listener.TCPListenerConfig;
import org.apache.bifromq.starter.config.model.mqtt.listener.TLSListenerConfig;
import org.apache.bifromq.starter.config.model.mqtt.listener.WSListenerConfig;
import org.apache.bifromq.starter.config.model.mqtt.listener.WSSListenerConfig;
import org.apache.bifromq.starter.config.model.serde.StructMapDeserializer;
import org.apache.bifromq.starter.config.model.serde.StructMapSerializer;

@Getter
@Setter
public class MQTTServerConfig {
    private boolean enable = true;
    private int connTimeoutSec = 20;
    private int maxConnPerSec = 2000;
    private int maxDisconnPerSec = 1000;
    private int maxMsgByteSize = 256 * 1024;
    private int maxConnBandwidth = 512 * 1024;
    private int bossELGThreads = 1;
    private int workerELGThreads = Math.max(2, EnvProvider.INSTANCE.availableProcessors() / 2);

    @JsonSerialize(using = StructMapSerializer.class)
    @JsonDeserialize(using = StructMapDeserializer.class)
    @JsonSetter(nulls = Nulls.SKIP)
    private Map<String, Struct> userPropsCustomizerFactoryConfig = new HashMap<>();
    @JsonSetter(nulls = Nulls.SKIP)
    @JsonMerge
    private TCPListenerConfig tcpListener = new TCPListenerConfig();
    @JsonSetter(nulls = Nulls.SKIP)
    @JsonMerge
    private TLSListenerConfig tlsListener = new TLSListenerConfig();
    @JsonSetter(nulls = Nulls.SKIP)
    @JsonMerge
    private WSListenerConfig wsListener = new WSListenerConfig();
    @JsonSetter(nulls = Nulls.SKIP)
    @JsonMerge
    private WSSListenerConfig wssListener = new WSSListenerConfig();
}
