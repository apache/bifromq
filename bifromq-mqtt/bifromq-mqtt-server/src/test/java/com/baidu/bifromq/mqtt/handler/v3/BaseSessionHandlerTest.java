package com.baidu.bifromq.mqtt.handler.v3;

import static com.baidu.bifromq.plugin.settingprovider.Setting.ByPassPermCheckError;
import static com.baidu.bifromq.plugin.settingprovider.Setting.DebugModeEnabled;
import static com.baidu.bifromq.plugin.settingprovider.Setting.ForceTransient;
import static com.baidu.bifromq.plugin.settingprovider.Setting.InBoundBandWidth;
import static com.baidu.bifromq.plugin.settingprovider.Setting.MaxTopicFiltersPerSub;
import static com.baidu.bifromq.plugin.settingprovider.Setting.MaxTopicLength;
import static com.baidu.bifromq.plugin.settingprovider.Setting.MaxTopicLevelLength;
import static com.baidu.bifromq.plugin.settingprovider.Setting.MaxTopicLevels;
import static com.baidu.bifromq.plugin.settingprovider.Setting.MaxUserPayloadBytes;
import static com.baidu.bifromq.plugin.settingprovider.Setting.MsgPubPerSec;
import static com.baidu.bifromq.plugin.settingprovider.Setting.OutBoundBandWidth;
import static com.baidu.bifromq.plugin.settingprovider.Setting.RetainEnabled;
import static com.baidu.bifromq.plugin.settingprovider.Setting.RetainMessageMatchLimit;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_CHANNEL_ID_KEY;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_CLIENT_ADDRESS_KEY;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_CLIENT_BROKER_KEY;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_CLIENT_ID_KEY;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_PROTOCOL_VER_3_1_1_VALUE;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_PROTOCOL_VER_KEY;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_TYPE_VALUE;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_USER_ID_KEY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.internal.junit.ArrayAsserts.assertArrayEquals;

import com.baidu.bifromq.dist.client.DistResult;
import com.baidu.bifromq.dist.client.IDistClient;
import com.baidu.bifromq.dist.client.MatchResult;
import com.baidu.bifromq.dist.client.UnmatchResult;
import com.baidu.bifromq.inbox.client.IInboxClient;
import com.baidu.bifromq.inbox.rpc.proto.AttachReply;
import com.baidu.bifromq.inbox.rpc.proto.CommitReply;
import com.baidu.bifromq.inbox.rpc.proto.CreateReply;
import com.baidu.bifromq.inbox.rpc.proto.DetachReply;
import com.baidu.bifromq.inbox.rpc.proto.ExpireReply;
import com.baidu.bifromq.inbox.rpc.proto.GetReply;
import com.baidu.bifromq.inbox.storage.proto.Fetched;
import com.baidu.bifromq.inbox.storage.proto.Fetched.Builder;
import com.baidu.bifromq.inbox.storage.proto.InboxMessage;
import com.baidu.bifromq.inbox.storage.proto.InboxVersion;
import com.baidu.bifromq.inbox.storage.proto.TopicFilterOption;
import com.baidu.bifromq.mqtt.MockableTest;
import com.baidu.bifromq.mqtt.service.ILocalDistService;
import com.baidu.bifromq.mqtt.service.ILocalSessionRegistry;
import com.baidu.bifromq.mqtt.session.MQTTSessionContext;
import com.baidu.bifromq.mqtt.utils.TestTicker;
import com.baidu.bifromq.plugin.authprovider.IAuthProvider;
import com.baidu.bifromq.plugin.authprovider.type.CheckResult;
import com.baidu.bifromq.plugin.authprovider.type.Denied;
import com.baidu.bifromq.plugin.authprovider.type.Granted;
import com.baidu.bifromq.plugin.eventcollector.Event;
import com.baidu.bifromq.plugin.eventcollector.EventType;
import com.baidu.bifromq.plugin.eventcollector.IEventCollector;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import com.baidu.bifromq.plugin.settingprovider.Setting;
import com.baidu.bifromq.retain.client.IRetainClient;
import com.baidu.bifromq.retain.rpc.proto.MatchReply;
import com.baidu.bifromq.retain.rpc.proto.RetainReply;
import com.baidu.bifromq.sessiondict.client.ISessionDictClient;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.MatchInfo;
import com.baidu.bifromq.type.Message;
import com.baidu.bifromq.type.QoS;
import com.baidu.bifromq.type.TopicMessage;
import com.baidu.bifromq.type.TopicMessagePack;
import com.bifromq.plugin.resourcethrottler.IResourceThrottler;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.mqtt.MqttSubAckMessage;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import lombok.SneakyThrows;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.stubbing.OngoingStubbing;
import org.testng.annotations.AfterMethod;

public class BaseSessionHandlerTest extends MockableTest {

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
    protected MQTTSessionContext sessionContext;
    protected final String topic = "topic";
    protected final String topicFilter = "testTopicFilter";
    protected final TestTicker testTicker = new TestTicker();
    protected Consumer<Fetched> inboxFetchConsumer;
    protected List<Integer> fetchHints = new ArrayList<>();

    @SneakyThrows
    @AfterMethod(alwaysRun = true)
    public void tearDown(Method method) {
        super.tearDown(method);
        fetchHints.clear();
        channel.close();
    }

    protected void verifySubAck(MqttSubAckMessage subAckMessage, int[] expectedQos) {
        assertEquals(subAckMessage.payload().grantedQoSLevels().size(), expectedQos.length);
        for (int i = 0; i < expectedQos.length; i++) {
            assertEquals(expectedQos[i], (int) subAckMessage.payload().grantedQoSLevels().get(i));
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
        Mockito.lenient().when(settingProvider.provide(any(Setting.class), anyString())).thenAnswer(
            invocation -> ((Setting) invocation.getArgument(0)).current(invocation.getArgument(1)));
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
            when(distClient.unmatch(anyLong(), anyString(), anyString(), anyString(), anyString(), anyInt()));
        for (CompletableFuture<UnmatchResult> result : unsubResults) {
            ongoingStubbing = ongoingStubbing.thenReturn(result);
        }
    }

    protected void mockDistDist(boolean success) {
        when(distClient.pub(anyLong(), anyString(), any(), any(ClientInfo.class)))
            .thenReturn(CompletableFuture.completedFuture(success ? DistResult.OK : DistResult.ERROR));
    }

    protected void mockDistBackPressure() {
        when(distClient.pub(anyLong(), anyString(), any(), any(ClientInfo.class)))
            .thenReturn(CompletableFuture.completedFuture(DistResult.BACK_PRESSURE_REJECTED));
    }

    protected void mockDistMatch(boolean success) {
        when(localDistService.match(anyLong(), anyString(), any()))
            .thenReturn(CompletableFuture.completedFuture(success ? MatchResult.OK : MatchResult.ERROR));
    }

    protected void mockDistMatch(String topic, boolean success) {
        when(distClient.match(anyLong(), anyString(), eq(topic), anyString(), anyString(), anyInt()))
            .thenReturn(CompletableFuture.completedFuture(success ? MatchResult.OK : MatchResult.ERROR));
    }

    protected MatchInfo matchInfo(String topicFilter) {
        return MatchInfo.newBuilder()
            .setTopicFilter(topicFilter)
            .setReceiverId("testInboxId")
            .build();
    }

    protected List<TopicMessagePack> s2cMessageList(String topic, int count, QoS qos) {
        List<TopicMessagePack> topicMessagePacks = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            topicMessagePacks.add(TopicMessagePack.newBuilder()
                .setTopic(topic)
                .addMessage(TopicMessagePack.PublisherPack.newBuilder()
                    .setPublisher(ClientInfo.newBuilder().build())
                    .addMessage(Message.newBuilder()
                        .setMessageId(i)
                        .setPayload(ByteString.EMPTY)
                        .setTimestamp(System.currentTimeMillis())
                        .setPubQoS(qos)
                        .build()))
                .build());
        }
        return topicMessagePacks;
    }

    protected List<TopicMessagePack> s2cMessageList(String topic, List<ByteBuffer> payloads, QoS qos) {
        List<TopicMessagePack> topicMessagePacks = new ArrayList<>();
        for (int i = 0; i < payloads.size(); i++) {
            topicMessagePacks.add(TopicMessagePack.newBuilder()
                .setTopic(topic)
                .addMessage(TopicMessagePack.PublisherPack.newBuilder()
                    .setPublisher(ClientInfo.newBuilder().build())
                    .addMessage(Message.newBuilder()
                        .setMessageId(i)
                        .setPayload(ByteString.copyFrom(payloads.get(i).duplicate()))
                        .setTimestamp(System.currentTimeMillis())
                        .setPubQoS(qos)
                        .build()))
                .build());
        }
        return topicMessagePacks;
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

    protected void mockInboxGet(InboxVersion... inboxVersions) {
        when(inboxClient.get(any()))
            .thenReturn(CompletableFuture.completedFuture(GetReply.newBuilder()
                .setCode(inboxVersions.length > 0 ? GetReply.Code.EXIST : GetReply.Code.NO_INBOX)
                .addAllInbox(List.of(inboxVersions))
                .build()));
    }

    protected void mockAttach(AttachReply.Code code) {
        when(inboxClient.attach(any()))
            .thenReturn(CompletableFuture.completedFuture(AttachReply.newBuilder().setCode(code).build()));
    }

    protected void mockDetach(DetachReply.Code code) {
        when(inboxClient.detach(any()))
            .thenReturn(CompletableFuture.completedFuture(DetachReply.newBuilder().setCode(code).build()));
    }

    protected void mockInboxCreate(boolean success) {
        when(inboxClient.create(any()))
            .thenReturn(CompletableFuture.completedFuture(CreateReply.newBuilder()
                .setCode(success ? CreateReply.Code.OK : CreateReply.Code.ERROR)
                .build())
            );
    }

    protected void mockInboxCreate(CreateReply.Code code) {
        when(inboxClient.create(any()))
            .thenReturn(CompletableFuture.completedFuture(CreateReply.newBuilder()
                .setCode(code)
                .build())
            );
    }

    protected void mockInboxExpire(boolean success) {
        when(inboxClient.expire(any()))
            .thenReturn(CompletableFuture.completedFuture(ExpireReply.newBuilder()
                .setCode(success ? ExpireReply.Code.OK : ExpireReply.Code.ERROR)
                .build()));
    }

    protected void mockInboxExpire(ExpireReply.Code code) {
        when(inboxClient.expire(any()))
            .thenReturn(CompletableFuture.completedFuture(ExpireReply.newBuilder()
                .setCode(code)
                .build()));
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
                .setTopicFilter(topicFilter)
                .setOption(TopicFilterOption.newBuilder().setQos(qoS).build())
                .setMsg(
                    TopicMessage.newBuilder()
                        .setTopic(topic)
                        .setMessage(
                            Message.newBuilder()
                                .setMessageId(i)
                                .setPayload(ByteString.copyFrom(bytes))
                                .setTimestamp(System.currentTimeMillis())
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
}
