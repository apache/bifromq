/*
 * Copyright (c) 2023. Baidu, Inc. All Rights Reserved.
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

package com.baidu.bifromq.starter.config.standalone.model.mqttserver;

import com.baidu.bifromq.baseenv.EnvProvider;
import com.baidu.bifromq.starter.config.standalone.model.mqttserver.listener.TCPListenerConfig;
import com.baidu.bifromq.starter.config.standalone.model.mqttserver.listener.TLSListenerConfig;
import com.baidu.bifromq.starter.config.standalone.model.mqttserver.listener.WSListenerConfig;
import com.baidu.bifromq.starter.config.standalone.model.mqttserver.listener.WSSListenerConfig;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class MQTTServerConfig {
    private int connTimeoutSec = 20;
    private int maxConnPerSec = 2000;
    private int maxDisconnPerSec = 1000;
    private int maxMsgByteSize = 256 * 1024;
    private int maxResendTimes = 5;
    private int maxConnBandwidth = 512 * 1024;
    private int defaultKeepAliveSec = 300;
    private int qos2ConfirmWindowSec = 5;
    private int bossELGThreads = 1;
    private int workerELGThreads = EnvProvider.INSTANCE.availableProcessors();

    @JsonSetter(nulls = Nulls.SKIP)
    private TCPListenerConfig tcpListener = new TCPListenerConfig();
    @JsonSetter(nulls = Nulls.SKIP)
    private TLSListenerConfig tlsListener = new TLSListenerConfig();
    @JsonSetter(nulls = Nulls.SKIP)
    private WSListenerConfig wsListener = new WSListenerConfig();
    @JsonSetter(nulls = Nulls.SKIP)
    private WSSListenerConfig wssListener = new WSSListenerConfig();
}
