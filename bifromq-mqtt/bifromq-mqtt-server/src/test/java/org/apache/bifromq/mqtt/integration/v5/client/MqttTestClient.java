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

package org.apache.bifromq.mqtt.integration.v5.client;

import com.google.protobuf.ByteString;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Single;
import io.reactivex.rxjava3.subjects.ReplaySubject;
import io.reactivex.rxjava3.subjects.SingleSubject;
import java.util.UUID;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.mqttv5.client.IMqttToken;
import org.eclipse.paho.mqttv5.client.MqttCallback;
import org.eclipse.paho.mqttv5.client.MqttClient;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions;
import org.eclipse.paho.mqttv5.client.MqttDisconnectResponse;
import org.eclipse.paho.mqttv5.client.persist.MemoryPersistence;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.eclipse.paho.mqttv5.common.packet.MqttProperties;

@Slf4j
public class MqttTestClient {
    private final MqttClient client;
    private final ReplaySubject<MqttMsg> messageSubject = ReplaySubject.create();
    private final SingleSubject<MqttDisconnectResponse> disconnectSubject = SingleSubject.create();
    private final ReplaySubject<MqttResponse> responseSubject = ReplaySubject.create();
    private final ReplaySubject<MqttException> errorSubject = ReplaySubject.create();


    @SneakyThrows
    public MqttTestClient(String brokerURI, String clientId) {
        client = new MqttClient(brokerURI, clientId, new MemoryPersistence());
        client.setTimeToWait(10000);
    }

    public MqttTestClient(String brokerURI) {
        this(brokerURI, UUID.randomUUID().toString());
    }

    public void manualAck(boolean manual) {
        client.setManualAcks(manual);
    }

    public boolean isConnected() {
        return client.isConnected();
    }

    @SneakyThrows
    public IMqttToken connect(MqttConnectionOptions options) {
        client.setCallback(new MqttCallback() {

            @Override
            public void disconnected(MqttDisconnectResponse disconnectResponse) {
                disconnectSubject.onSuccess(disconnectResponse);
            }

            @Override
            public void mqttErrorOccurred(MqttException exception) {
                errorSubject.onNext(exception);
            }

            @Override
            public void deliveryComplete(IMqttToken token) {
                try {
                    if (token.getResponse() != null) {
                        responseSubject.onNext(new MqttResponse(token.getResponse().getType(),
                            token.getResponse().getMessageId()));
                    }
                } catch (Exception exception) {
                    responseSubject.onError(exception);
                }
            }

            @Override
            public void connectComplete(boolean reconnect, String serverURI) {

            }

            @Override
            public void authPacketArrived(int reasonCode, MqttProperties properties) {

            }

            @Override
            public void messageArrived(String topic, MqttMessage message) {
                log.debug("Receive message from broker: {}", message);
                messageSubject.onNext(new MqttMsg(topic, message));
            }
        });
        return client.connectWithResult(options);
    }

    @SneakyThrows
    public void disconnect() {
        client.disconnect();
    }

    public Single<MqttDisconnectResponse> onDisconnect() {
        return disconnectSubject;
    }

    @SneakyThrows
    public void close() {
        client.close();
    }

    public void closeForcibly() {
        try {
            client.disconnectForcibly();
            client.close(true);
        } catch (Throwable e) {
            // ignore intentionally;
        }
    }

//    Observable<MqttMsg> subscribe(String topicFilter, int qos) {
//        Observable<MqttMsg> orig = Observable.create(emitter ->
//                client.subscribe(topicFilter, qos, (topic, message) -> emitter.onNext(new MqttMsg(topic, message))));
//        Observable<MqttMsg> replayed = orig.cache();
//        replayed.subscribe().dispose();// trigger the sub action
//        return replayed.doOnDispose(() -> client.unsubscribe(topicFilter)).share();
//    }

    @SneakyThrows
    public Observable<MqttMsg> subscribe(String topicFilter, int qos) {
        client.subscribe(topicFilter, qos);
        return messageSubject;
    }


    public Observable<MqttMsg> messageArrived() {
        return messageSubject;
    }

    public Observable<MqttResponse> deliveryComplete() {
        return responseSubject;
    }

    @SneakyThrows
    public void unsubscribe(String topicFilter) {
        client.unsubscribe(topicFilter);
    }

    @SneakyThrows
    public void ack(int messageId, int qos) {
        client.messageArrivedComplete(messageId, qos);
    }

    @SneakyThrows
    public void publish(String topic, int qos, ByteString payload, boolean retain) {
        client.publish(topic, payload.toByteArray(), qos, retain);
    }

    @SneakyThrows
    public void publish(String topic, MqttMessage message) {
        client.publish(topic, message);
    }
}
