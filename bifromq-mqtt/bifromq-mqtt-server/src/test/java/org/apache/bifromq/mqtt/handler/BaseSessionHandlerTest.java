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

package org.apache.bifromq.mqtt.handler;

import static org.apache.bifromq.plugin.settingprovider.Setting.ByPassPermCheckError;
import static org.apache.bifromq.plugin.settingprovider.Setting.DebugModeEnabled;
import static org.apache.bifromq.plugin.settingprovider.Setting.ForceTransient;
import static org.apache.bifromq.plugin.settingprovider.Setting.InBoundBandWidth;
import static org.apache.bifromq.plugin.settingprovider.Setting.MaxTopicFiltersPerInbox;
import static org.apache.bifromq.plugin.settingprovider.Setting.MaxTopicFiltersPerSub;
import static org.apache.bifromq.plugin.settingprovider.Setting.MaxTopicLength;
import static org.apache.bifromq.plugin.settingprovider.Setting.MaxTopicLevelLength;
import static org.apache.bifromq.plugin.settingprovider.Setting.MaxTopicLevels;
import static org.apache.bifromq.plugin.settingprovider.Setting.MaxUserPayloadBytes;
import static org.apache.bifromq.plugin.settingprovider.Setting.MsgPubPerSec;
import static org.apache.bifromq.plugin.settingprovider.Setting.OutBoundBandWidth;
import static org.apache.bifromq.plugin.settingprovider.Setting.RetainEnabled;
import static org.apache.bifromq.plugin.settingprovider.Setting.RetainMessageMatchLimit;
import static org.apache.bifromq.type.MQTTClientInfoConstants.MQTT_CHANNEL_ID_KEY;
import static org.apache.bifromq.type.MQTTClientInfoConstants.MQTT_CLIENT_ADDRESS_KEY;
import static org.apache.bifromq.type.MQTTClientInfoConstants.MQTT_CLIENT_BROKER_KEY;
import static org.apache.bifromq.type.MQTTClientInfoConstants.MQTT_CLIENT_ID_KEY;
import static org.apache.bifromq.type.MQTTClientInfoConstants.MQTT_PROTOCOL_VER_3_1_1_VALUE;
import static org.apache.bifromq.type.MQTTClientInfoConstants.MQTT_PROTOCOL_VER_KEY;
import static org.apache.bifromq.type.MQTTClientInfoConstants.MQTT_TYPE_VALUE;
import static org.apache.bifromq.type.MQTTClientInfoConstants.MQTT_USER_ID_KEY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.internal.junit.ArrayAsserts.assertArrayEquals;

import org.apache.bifromq.plugin.authprovider.IAuthProvider;
import org.apache.bifromq.plugin.authprovider.type.CheckResult;
import org.apache.bifromq.plugin.authprovider.type.Denied;
import org.apache.bifromq.plugin.authprovider.type.Granted;
import org.apache.bifromq.plugin.clientbalancer.IClientBalancer;
import org.apache.bifromq.plugin.eventcollector.Event;
import org.apache.bifromq.plugin.eventcollector.EventType;
import org.apache.bifromq.plugin.eventcollector.IEventCollector;
import org.apache.bifromq.plugin.settingprovider.ISettingProvider;
import org.apache.bifromq.plugin.settingprovider.Setting;
import org.apache.bifromq.retain.client.IRetainClient;
import org.apache.bifromq.retain.rpc.proto.MatchReply;
import org.apache.bifromq.retain.rpc.proto.RetainReply;
import org.apache.bifromq.plugin.resourcethrottler.IResourceThrottler;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import io.micrometer.core.instrument.Timer;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.mqtt.MqttSubAckMessage;
import io.netty.handler.codec.mqtt.MqttUnsubAckMessage;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.bifromq.basehlc.HLC;
import org.apache.bifromq.dist.client.IDistClient;
import org.apache.bifromq.dist.client.MatchResult;
import org.apache.bifromq.dist.client.PubResult;
import org.apache.bifromq.dist.client.UnmatchResult;
import org.apache.bifromq.inbox.client.IInboxClient;
import org.apache.bifromq.inbox.rpc.proto.CommitReply;
import org.apache.bifromq.inbox.rpc.proto.SubReply;
import org.apache.bifromq.inbox.storage.proto.Fetched;
import org.apache.bifromq.inbox.storage.proto.Fetched.Builder;
import org.apache.bifromq.inbox.storage.proto.InboxMessage;
import org.apache.bifromq.inbox.storage.proto.TopicFilterOption;
import org.apache.bifromq.metrics.ITenantMeter;
import org.apache.bifromq.mqtt.MockableTest;
import org.apache.bifromq.mqtt.handler.condition.Condition;
import org.apache.bifromq.mqtt.service.ILocalDistService;
import org.apache.bifromq.mqtt.service.ILocalSessionRegistry;
import org.apache.bifromq.mqtt.session.MQTTSessionContext;
import org.apache.bifromq.mqtt.utils.TestTicker;
import org.apache.bifromq.sessiondict.client.ISessionDictClient;
import org.apache.bifromq.sessiondict.client.ISessionRegistration;
import org.apache.bifromq.type.ClientInfo;
import org.apache.bifromq.type.Message;
import org.apache.bifromq.type.QoS;
import org.apache.bifromq.type.TopicMessage;
import org.apache.bifromq.type.TopicMessagePack;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.mockito.stubbing.OngoingStubbing;

public abstract class BaseSessionHandlerTest extends MockableTest {

    protected final String tenantId = "tenantId";
    protected final String serverId = "serverId";
    protected final String remoteIp = "127.0.0.1";
    protected final int remotePort = 8888;
    protected final ClientInfo clientInfo = ClientInfo.newBuilder()
        .setTenantId(tenantId)
        .setType(MQTT_TYPE_VALUE)
        .putMetadata(MQTT_PROTOCOL_VER_KEY, MQTT_PROTOCOL_VER_3_1_1_VALUE)
        .putMetadata(MQTT_USER_ID_KEY, "userId")
        .putMetadata(MQTT_CLIENT_ID_KEY, "clientId")
        .putMetadata(MQTT_CHANNEL_ID_KEY, "channelId")
        .putMetadata(MQTT_CLIENT_ADDRESS_KEY, new InetSocketAddress(remoteIp, remotePort).toString())
        .putMetadata(MQTT_CLIENT_BROKER_KEY, serverId)
        .build();
    protected final String topic = "topic";
    protected final String topicFilter = "topic/#";
    protected final TestTicker testTicker = new TestTicker();
    protected EmbeddedChannel channel;
    @Mock
    protected ILocalDistService localDistService;
    @Mock
    protected IDistClient distClient;
    @Mock
    protected IRetainClient retainClient;
    @Mock
    protected ISessionDictClient sessionDictClient;
    @Mock
    protected ILocalSessionRegistry localSessionRegistry;
    @Mock
    protected ISessionRegistration sessionRegister;
    @Mock
    protected IAuthProvider authProvider;
    @Mock
    protected IInboxClient inboxClient;
    @Mock
    protected IEventCollector eventCollector;
    @Mock
    protected IResourceThrottler resourceThrottler;
    @Mock
    protected ISettingProvider settingProvider;
    @Mock
    protected IInboxClient.IInboxReader inboxReader;
    @Mock
    protected ITenantMeter tenantMeter;
    @Mock
    protected Condition oomCondition;
    @Mock
    protected IClientBalancer clientBalancer;
    protected MQTTSessionContext sessionContext;
    protected Consumer<Fetched> inboxFetchConsumer;
    protected List<Integer> fetchHints = new ArrayList<>();
    protected AtomicReference<ISessionDictClient.IKillListener> onKill = new AtomicReference<>();

    public void setup(Method method) {
        super.setup(method);
        when(tenantMeter.timer(any())).thenReturn(mock(Timer.class));
        when(oomCondition.meet()).thenReturn(false);
        when(clientBalancer.needRedirect(any())).thenReturn(Optional.empty());
        sessionContext = MQTTSessionContext.builder()
            .serverId(serverId)
            .ticker(testTicker)
            .distClient(distClient)
            .retainClient(retainClient)
            .authProvider(authProvider)
            .localDistService(localDistService)
            .localSessionRegistry(localSessionRegistry)
            .sessionDictClient(sessionDictClient)
            .clientBalancer(clientBalancer)
            .inboxClient(inboxClient)
            .eventCollector(eventCollector)
            .resourceThrottler(resourceThrottler)
            .settingProvider(settingProvider)
            .build();
        mockSettings();
    }

    protected abstract ChannelDuplexHandler buildChannelHandler();

    protected void verifySubAck(MqttSubAckMessage subAckMessage, int[] expectedReasonCodes) {
        assertEquals(subAckMessage.payload().reasonCodes().size(), expectedReasonCodes.length);
        for (int i = 0; i < expectedReasonCodes.length; i++) {
            assertEquals((int) subAckMessage.payload().reasonCodes().get(i), expectedReasonCodes[i]);
        }
    }

    protected void verifyMQTT5UnSubAck(MqttUnsubAckMessage unsubAckMessage, int[] expectedReasonCodes) {
        assertEquals(unsubAckMessage.payload().unsubscribeReasonCodes().size(), expectedReasonCodes.length);
        for (int i = 0; i < expectedReasonCodes.length; i++) {
            assertEquals((int) unsubAckMessage.payload().unsubscribeReasonCodes().get(i), expectedReasonCodes[i]);
        }
    }

    protected void verifyEvent(EventType... types) {
        ArgumentCaptor<Event> eventArgumentCaptor = ArgumentCaptor.forClass(Event.class);
        verify(eventCollector, times(types.length)).report(eventArgumentCaptor.capture());
        if (types.length != 0) {
            assertArrayEquals(types, eventArgumentCaptor.getAllValues().stream().map(Event::type).toArray());
        }
    }

    protected void mockSettings() {
        Mockito.lenient().when(resourceThrottler.hasResource(anyString(), any())).thenReturn(true);
        Mockito.lenient().when(settingProvider.provide(any(Setting.class), anyString()))
            .thenAnswer(invocation -> {
                Setting setting = invocation.getArgument(0);
                switch (setting) {
                    case MinKeepAliveSeconds -> {
                        return 2;
                    }
                    default -> {
                        return ((Setting) invocation.getArgument(0)).current(invocation.getArgument(1));
                    }
                }
            });
        Mockito.lenient().when(settingProvider.provide(eq(InBoundBandWidth), anyString())).thenReturn(51200 * 1024L);
        Mockito.lenient().when(settingProvider.provide(eq(OutBoundBandWidth), anyString())).thenReturn(51200 * 1024L);
        Mockito.lenient().when(settingProvider.provide(eq(ForceTransient), anyString())).thenReturn(false);
        Mockito.lenient().when(settingProvider.provide(eq(MaxUserPayloadBytes), anyString())).thenReturn(256 * 1024);
        Mockito.lenient().when(settingProvider.provide(eq(MaxTopicLevelLength), anyString())).thenReturn(40);
        Mockito.lenient().when(settingProvider.provide(eq(MaxTopicLevels), anyString())).thenReturn(16);
        Mockito.lenient().when(settingProvider.provide(eq(MaxTopicLength), anyString())).thenReturn(255);
        Mockito.lenient().when(settingProvider.provide(eq(ByPassPermCheckError), anyString())).thenReturn(true);
        Mockito.lenient().when(settingProvider.provide(eq(MsgPubPerSec), anyString())).thenReturn(200);
        Mockito.lenient().when(settingProvider.provide(eq(DebugModeEnabled), anyString())).thenReturn(true);
        Mockito.lenient().when(settingProvider.provide(eq(RetainEnabled), anyString())).thenReturn(true);
        Mockito.lenient().when(settingProvider.provide(eq(RetainMessageMatchLimit), anyString())).thenReturn(10);
        Mockito.lenient().when(settingProvider.provide(eq(MaxTopicFiltersPerSub), anyString())).thenReturn(10);
        Mockito.lenient().when(settingProvider.provide(eq(MaxTopicFiltersPerInbox), anyString())).thenReturn(10);
    }

    protected void mockCheckPermission(boolean allow) {
        when(authProvider.checkPermission(any(ClientInfo.class), any()))
            .thenReturn(CompletableFuture.completedFuture(allow ?
                CheckResult.newBuilder()
                    .setGranted(Granted.getDefaultInstance())
                    .build() :
                CheckResult.newBuilder()
                    .setDenied(Denied.getDefaultInstance())
                    .build()));
    }

    protected void mockDistUnMatch(boolean... success) {
        CompletableFuture<UnmatchResult>[] unsubResults = new CompletableFuture[success.length];
        for (int i = 0; i < success.length; i++) {
            unsubResults[i] = success[i] ? CompletableFuture.completedFuture(UnmatchResult.OK)
                : CompletableFuture.failedFuture(new RuntimeException("InternalError"));
        }
        OngoingStubbing<CompletableFuture<UnmatchResult>> ongoingStubbing =
            when(localDistService.unmatch(anyLong(), anyString(), anyLong(), any()));
        for (CompletableFuture<UnmatchResult> result : unsubResults) {
            ongoingStubbing = ongoingStubbing.thenReturn(result);
        }
    }

    protected void mockDistDist(boolean success) {
        when(distClient.pub(anyLong(), anyString(), any(), any(ClientInfo.class)))
            .thenReturn(CompletableFuture.completedFuture(success ? PubResult.OK : PubResult.ERROR));
    }

    protected void mockDistBackPressure() {
        when(distClient.pub(anyLong(), anyString(), any(), any(ClientInfo.class)))
            .thenReturn(CompletableFuture.completedFuture(PubResult.BACK_PRESSURE_REJECTED));
    }

    protected void mockDistMatch(boolean success) {
        when(localDistService.match(anyLong(), anyString(), anyLong(), any()))
            .thenReturn(CompletableFuture.completedFuture(success ? MatchResult.OK : MatchResult.ERROR));
    }

    protected void mockDistMatch(String topicFilter, boolean success) {
        when(localDistService.match(anyLong(), eq(topicFilter), anyLong(), any()))
            .thenReturn(CompletableFuture.completedFuture(success ? MatchResult.OK : MatchResult.ERROR));
    }

    protected TopicMessagePack s2cMQTT5MessageList(String topic, int count, QoS qos) {
        TopicMessagePack.Builder packBuilder = TopicMessagePack.newBuilder().setTopic(topic);
        for (int i = 0; i < count; i++) {
            packBuilder.addMessage(TopicMessagePack.PublisherPack.newBuilder()
                .setPublisher(ClientInfo.newBuilder().build())
                .addMessage(Message.newBuilder()
                    .setMessageId(i)
                    .setExpiryInterval(Integer.MAX_VALUE)
                    .setPayload(ByteString.EMPTY)
                    .setTimestamp(HLC.INST.get())
                    .setPubQoS(qos)
                    .build()));
        }
        return packBuilder.build();
    }

    protected TopicMessagePack s2cMQTT5MessageList(String topic, List<ByteBuffer> payloads, QoS qos) {
        TopicMessagePack.Builder packBuilder = TopicMessagePack.newBuilder().setTopic(topic);
        for (int i = 0; i < payloads.size(); i++) {
            packBuilder.addMessage(TopicMessagePack.PublisherPack.newBuilder()
                .setPublisher(ClientInfo.newBuilder().build())
                .addMessage(Message.newBuilder()
                    .setMessageId(i)
                    .setExpiryInterval(Integer.MAX_VALUE)
                    .setPayload(ByteString.copyFrom(payloads.get(i).duplicate()))
                    .setTimestamp(HLC.INST.get())
                    .setPubQoS(qos)
                    .build()));
        }
        return packBuilder.build();
    }

    protected TopicMessagePack s2cMessageList(String topic, int count, QoS qos) {
        TopicMessagePack.Builder packBuilder = TopicMessagePack.newBuilder().setTopic(topic);
        for (int i = 0; i < count; i++) {
            packBuilder.addMessage(TopicMessagePack.PublisherPack.newBuilder()
                .setPublisher(ClientInfo.newBuilder().build())
                .addMessage(Message.newBuilder()
                    .setMessageId(i)
                    .setPayload(ByteString.EMPTY)
                    .setTimestamp(HLC.INST.get())
                    .setPubQoS(qos)
                    .build()));
        }
        return packBuilder.build();
    }

    protected TopicMessagePack s2cMessageList(String topic, List<ByteBuffer> payloads, QoS qos) {
        TopicMessagePack.Builder packBuilder = TopicMessagePack.newBuilder().setTopic(topic);
        for (int i = 0; i < payloads.size(); i++) {
            packBuilder.addMessage(TopicMessagePack.PublisherPack.newBuilder()
                    .setPublisher(ClientInfo.newBuilder().build())
                    .addMessage(Message.newBuilder()
                        .setMessageId(i)
                        .setPayload(ByteString.copyFrom(payloads.get(i).duplicate()))
                        .setTimestamp(HLC.INST.get())
                        .setPubQoS(qos)
                        .build()))
                .build();
        }
        return packBuilder.build();
    }

    protected List<ByteBuffer> s2cMessagesPayload(int count, int size) {
        List<ByteBuffer> list = Lists.newArrayList();
        for (int i = 0; i < count; i++) {
            byte[] bytes = new byte[size];
            Arrays.fill(bytes, (byte) 1);
            list.add(ByteBuffer.wrap(bytes));
        }
        return list;
    }

    protected void mockInboxCommit(CommitReply.Code code) {
        when(inboxClient.commit(any()))
            .thenReturn(
                CompletableFuture.completedFuture(CommitReply.newBuilder().setCode(code).build()));
    }

    protected void mockInboxReader() {
        when(inboxClient.openInboxReader(anyString(), anyString(), anyLong())).thenReturn(inboxReader);
        doAnswer(invocationOnMock -> {
            inboxFetchConsumer = invocationOnMock.getArgument(0);
            return null;
        }).when(inboxReader).fetch(any(Consumer.class));
        lenient().doAnswer(invocationOnMock -> {
            fetchHints.add(invocationOnMock.getArgument(0));
            return null;
        }).when(inboxReader).hint(anyInt());
    }

    protected Fetched fetch(int count, int payloadSize, QoS qoS) {
        Builder builder = Fetched.newBuilder();
        byte[] bytes = new byte[payloadSize];
        Arrays.fill(bytes, (byte) 1);
        for (int i = 0; i < count; i++) {
            InboxMessage inboxMessage = InboxMessage.newBuilder()
                .setSeq(i)
                .putMatchedTopicFilter(topicFilter, TopicFilterOption.newBuilder().setQos(qoS).build())
                .setMsg(
                    TopicMessage.newBuilder()
                        .setTopic(topic)
                        .setMessage(
                            Message.newBuilder()
                                .setMessageId(i)
                                .setPayload(ByteString.copyFrom(bytes))
                                .setTimestamp(HLC.INST.get())
                                .setExpiryInterval(120)
                                .setPubQoS(qoS)
                                .build()
                        )
                        .setPublisher(
                            ClientInfo.newBuilder()
                                .setType(MQTT_TYPE_VALUE)
                                .build()
                        )
                        .build()
                ).build();
            switch (qoS) {
                case AT_MOST_ONCE -> builder.addQos0Msg(inboxMessage);
                case AT_LEAST_ONCE, EXACTLY_ONCE -> builder.addSendBufferMsg(inboxMessage);
            }
        }
        return builder.build();
    }

    protected void mockRetainMatch() {
        when(retainClient.match(any()))
            .thenReturn(CompletableFuture.completedFuture(
                MatchReply.newBuilder().setResult(MatchReply.Result.OK).build()
            ));
    }

    protected void mockRetainPipeline(RetainReply.Result result) {
        when(retainClient.retain(anyLong(), anyString(), any(QoS.class), any(ByteString.class), anyInt(),
            any(ClientInfo.class)))
            .thenReturn(CompletableFuture.completedFuture(RetainReply.newBuilder().setResult(result).build()));
    }

    protected void mockSessionReg() {
        when(sessionDictClient.reg(any(), any())).thenAnswer(
            (Answer<ISessionRegistration>) invocation -> {
                onKill.set(invocation.getArgument(1));
                return sessionRegister;
            });
    }

    protected void mockInboxSub(QoS qos, boolean success) {
        when(inboxClient.sub(any())).thenReturn(CompletableFuture.completedFuture(
            SubReply.newBuilder()
                .setCode(success ? SubReply.Code.OK : SubReply.Code.ERROR)
                .build()));
    }

    protected void mockDistUnmatch(boolean... success) {
        CompletableFuture<UnmatchResult>[] unsubResults = new CompletableFuture[success.length];
        for (int i = 0; i < success.length; i++) {
            unsubResults[i] = success[i] ? CompletableFuture.completedFuture(UnmatchResult.OK)
                : CompletableFuture.failedFuture(new RuntimeException("InternalError"));
        }
        OngoingStubbing<CompletableFuture<UnmatchResult>> ongoingStubbing =
            when(distClient
                .removeRoute(anyLong(), anyString(), any(), anyString(), anyString(), anyInt(), anyLong()));
        for (CompletableFuture<UnmatchResult> result : unsubResults) {
            ongoingStubbing = ongoingStubbing.thenReturn(result);
        }
    }
}
