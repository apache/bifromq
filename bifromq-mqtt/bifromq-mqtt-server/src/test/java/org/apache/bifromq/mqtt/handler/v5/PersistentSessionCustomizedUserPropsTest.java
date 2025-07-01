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

package org.apache.bifromq.mqtt.handler.v5;

import static org.apache.bifromq.mqtt.handler.MQTTSessionIdUtil.userSessionId;
import static org.apache.bifromq.mqtt.handler.v5.MQTT5MessageUtils.toUserProperties;
import static org.apache.bifromq.plugin.eventcollector.EventType.QOS0_PUSHED;
import static org.apache.bifromq.type.MQTTClientInfoConstants.MQTT_TYPE_VALUE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.google.protobuf.ByteString;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttSubAckMessage;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttVersion;
import io.netty.handler.traffic.ChannelTrafficShapingHandler;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.SneakyThrows;
import org.apache.bifromq.basehlc.HLC;
import org.apache.bifromq.inbox.rpc.proto.CommitReply;
import org.apache.bifromq.inbox.storage.proto.Fetched;
import org.apache.bifromq.inbox.storage.proto.InboxMessage;
import org.apache.bifromq.inbox.storage.proto.InboxVersion;
import org.apache.bifromq.mqtt.handler.BaseSessionHandlerTest;
import org.apache.bifromq.mqtt.handler.ChannelAttrs;
import org.apache.bifromq.mqtt.handler.TenantSettings;
import org.apache.bifromq.mqtt.spi.UserProperty;
import org.apache.bifromq.mqtt.utils.MQTTMessageUtils;
import org.apache.bifromq.plugin.authprovider.type.CheckResult;
import org.apache.bifromq.plugin.authprovider.type.Granted;
import org.apache.bifromq.type.ClientInfo;
import org.apache.bifromq.type.Message;
import org.apache.bifromq.type.QoS;
import org.apache.bifromq.type.StringPair;
import org.apache.bifromq.type.TopicFilterOption;
import org.apache.bifromq.type.TopicMessage;
import org.apache.bifromq.type.UserProperties;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class PersistentSessionCustomizedUserPropsTest extends BaseSessionHandlerTest {

    @BeforeMethod(alwaysRun = true)
    public void setup(Method method) {
        super.setup(method);
        ChannelDuplexHandler sessionHandlerAdder = buildChannelHandler();
        mockInboxReader();
        channel = new EmbeddedChannel(true, true, new ChannelInitializer<>() {
            @Override
            protected void initChannel(Channel ch) {
                ch.attr(ChannelAttrs.MQTT_SESSION_CTX).set(sessionContext);
                ch.attr(ChannelAttrs.PEER_ADDR).set(new InetSocketAddress(remoteIp, remotePort));
                ChannelPipeline pipeline = ch.pipeline();
                pipeline.addLast(new ChannelTrafficShapingHandler(512 * 1024, 512 * 1024));
                pipeline.addLast(MqttDecoder.class.getName(), new MqttDecoder(256 * 1024));
                pipeline.addLast(sessionHandlerAdder);
            }
        });
    }

    @SneakyThrows
    @AfterMethod(alwaysRun = true)
    public void tearDown(Method method) {
        super.tearDown(method);
        fetchHints.clear();
    }

    @Override
    protected ChannelDuplexHandler buildChannelHandler() {
        return new ChannelDuplexHandler() {
            @Override
            public void channelActive(ChannelHandlerContext ctx) throws Exception {
                super.channelActive(ctx);
                ctx.pipeline().addLast(MQTT5PersistentSessionHandler.builder()
                    .settings(new TenantSettings(tenantId, settingProvider))
                    .tenantMeter(tenantMeter)
                    .oomCondition(oomCondition)
                    .inboxVersion(InboxVersion.newBuilder()
                        .setMod(0)
                        .setIncarnation(0)
                        .build())
                    .connMsg(MqttMessageBuilders.connect()
                        .protocolVersion(MqttVersion.MQTT_5)
                        .build())
                    .userSessionId(userSessionId(clientInfo))
                    .keepAliveTimeSeconds(120)
                    .sessionExpirySeconds(120)
                    .clientInfo(clientInfo)
                    .noDelayLWT(null)
                    .ctx(ctx)
                    .build());
                ctx.pipeline().remove(this);
            }
        };
    }

    @Test
    public void grantUserPropsForSub() {
        UserProperties grantedUserProps = UserProperties.newBuilder()
            .addUserProperties(StringPair.newBuilder()
                .setKey("key1")
                .setValue("value1")
                .build())
            .build();
        when(authProvider.checkPermission(any(ClientInfo.class), any()))
            .thenReturn(CompletableFuture.completedFuture(
                CheckResult.newBuilder()
                    .setGranted(Granted.newBuilder()
                        .setUserProps(grantedUserProps)
                        .build())
                    .build()));
        mockInboxSub(QoS.AT_MOST_ONCE, true);
        mockRetainMatch();
        int[] qos = {0};
        MqttSubscribeMessage subMessage = MQTTMessageUtils.qoSMqttSubMessages(qos);
        channel.writeInbound(subMessage);
        MqttSubAckMessage subAckMessage = channel.readOutbound();
        verifySubAck(subAckMessage, qos);
        verify(inboxClient).sub(argThat(sub -> sub.getOption().getUserProperties().equals(grantedUserProps)));
    }

    @Test
    public void extraOutboundUserProps() {
        mockInboxCommit(CommitReply.Code.OK);
        mockCheckPermission(true);
        when(userPropsCustomizer.outbound(anyString(), any(), any(), anyString(), any(), any(), anyLong()))
            .thenReturn(List.of(new UserProperty("key1", "value1")));

        Fetched.Builder builder = Fetched.newBuilder();
        byte[] bytes = new byte[64];
        Arrays.fill(bytes, (byte) 1);
        InboxMessage inboxMessage = InboxMessage.newBuilder()
            .setSeq(0)
            .putMatchedTopicFilter(topicFilter, TopicFilterOption.newBuilder().setQos(QoS.AT_MOST_ONCE).build())
            .setMsg(
                TopicMessage.newBuilder()
                    .setTopic(topic)
                    .setMessage(
                        Message.newBuilder()
                            .setMessageId(0)
                            .setPayload(ByteString.copyFrom(bytes))
                            .setTimestamp(HLC.INST.get())
                            .setExpiryInterval(120)
                            .setPubQoS(QoS.AT_MOST_ONCE)
                            .build()
                    )
                    .setPublisher(
                        ClientInfo.newBuilder()
                            .setType(MQTT_TYPE_VALUE)
                            .build()
                    )
                    .build()
            ).build();
        Fetched fetched = builder.addQos0Msg(inboxMessage).build();
        inboxFetchConsumer.accept(fetched);
        channel.runPendingTasks();
        MqttPublishMessage message = channel.readOutbound();
        UserProperties userProperties = toUserProperties(message.variableHeader().properties());
        assertEquals(userProperties.getUserPropertiesList().size(), 1);
        StringPair userProperty = userProperties.getUserPropertiesList().get(0);
        assertEquals(userProperty.getKey(), "key1");
        assertEquals(userProperty.getValue(), "value1");
        verifyEvent(QOS0_PUSHED);
        assertEquals(fetchHints.size(), 1);
    }
}
