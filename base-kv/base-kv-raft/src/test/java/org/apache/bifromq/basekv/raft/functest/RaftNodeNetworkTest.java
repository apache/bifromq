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

package org.apache.bifromq.basekv.raft.functest;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

import org.apache.bifromq.basekv.raft.IRaftNode;
import org.apache.bifromq.basekv.raft.proto.AppendEntries;
import org.apache.bifromq.basekv.raft.proto.RaftMessage;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RaftNodeNetworkTest {
    @Mock
    private IRaftNode raftNode1;
    @Mock
    private IRaftNode raftNode2;
    @Mock
    private IRaftNode raftNode3;

    private RaftNodeNetwork raftNodeNetwork;
    private AutoCloseable closeable;
    @BeforeMethod
    public void openMocks() {
        raftNodeNetwork = new RaftNodeNetwork();
        closeable = MockitoAnnotations.openMocks(this);
    }

    @AfterMethod
    public void releaseMocks() throws Exception {
        closeable.close();
    }
    @Test
    public void testConnect() {
        when(raftNode1.id()).thenReturn("V1");
        when(raftNode2.id()).thenReturn("V2");
        IRaftNode.IRaftMessageSender raftMessageListenerV1 = raftNodeNetwork.connect(raftNode1);
        assertEquals(raftNodeNetwork.connect(raftNode1), raftMessageListenerV1);
        assertNotEquals(raftNodeNetwork.connect(raftNode2), raftMessageListenerV1);
    }

    @Test
    public void testDisconnect() {
        when(raftNode1.id()).thenReturn("V1");
        when(raftNode2.id()).thenReturn("V2");
        when(raftNode2.isStarted()).thenReturn(true);
        when(raftNode2.stop()).thenReturn(CompletableFuture.completedFuture(null));
        IRaftNode.IRaftMessageSender raftMessageListenerV1 = raftNodeNetwork.connect(raftNode1);
        raftNodeNetwork.connect(raftNode2);
        List<RaftMessage> testMessages =
            Arrays.asList(RaftMessage.newBuilder().setTerm(1).build(), RaftMessage.newBuilder().setTerm(2).build());
        raftMessageListenerV1.send(new HashMap<String, List<RaftMessage>>() {{
            put("V2", testMessages);
        }});
        raftNodeNetwork.tick();

        ArgumentCaptor<String> fromPeers = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<RaftMessage> receivedMessages = ArgumentCaptor.forClass(RaftMessage.class);
        verify(raftNode2, times(2)).receive(fromPeers.capture(), receivedMessages.capture());
        assertEquals(fromPeers.getAllValues(), Arrays.asList("V1", "V1"));
        assertEquals(receivedMessages.getAllValues(), testMessages);

        raftNodeNetwork.disconnect("V2");
        raftMessageListenerV1.send(new HashMap<String, List<RaftMessage>>() {{
            put("V2", testMessages);
        }});
        raftNodeNetwork.tick();

        verify(raftNode2, times(2)).receive(anyString(), any(RaftMessage.class));
    }

    @Test
    public void testSend() {
        when(raftNode1.id()).thenReturn("V1");
        when(raftNode1.isStarted()).thenReturn(true);

        when(raftNode2.id()).thenReturn("V2");
        when(raftNode2.isStarted()).thenReturn(true);

        when(raftNode3.id()).thenReturn("V3");
        when(raftNode3.isStarted()).thenReturn(true);

        IRaftNode.IRaftMessageSender raftMessageListenerV1 = raftNodeNetwork.connect(raftNode1);
        IRaftNode.IRaftMessageSender raftMessageListenerV2 = raftNodeNetwork.connect(raftNode2);
        raftNodeNetwork.connect(raftNode3);
        List<RaftMessage> testMessages =
            Arrays.asList(RaftMessage.newBuilder().setTerm(1).build(), RaftMessage.newBuilder().setTerm(2).build());
        raftMessageListenerV1.send(new HashMap<String, List<RaftMessage>>() {{
            put("V2", testMessages);
            put("V3", testMessages);
        }});
        raftNodeNetwork.tick();


        ArgumentCaptor<String> fromPeers = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<RaftMessage> receivedMessages = ArgumentCaptor.forClass(RaftMessage.class);
        verify(raftNode2, times(2)).receive(fromPeers.capture(), receivedMessages.capture());
        assertEquals(fromPeers.getAllValues(), Arrays.asList("V1", "V1"));
        assertEquals(receivedMessages.getAllValues(), testMessages);

        fromPeers = ArgumentCaptor.forClass(String.class);
        receivedMessages = ArgumentCaptor.forClass(RaftMessage.class);
        verify(raftNode3, times(2)).receive(fromPeers.capture(), receivedMessages.capture());
        assertEquals(fromPeers.getAllValues(), Arrays.asList("V1", "V1"));
        assertEquals(receivedMessages.getAllValues(), testMessages);

        raftMessageListenerV2.send(new HashMap<String, List<RaftMessage>>() {{
            put("V1", testMessages);
        }});
        raftNodeNetwork.tick();

        fromPeers = ArgumentCaptor.forClass(String.class);
        receivedMessages = ArgumentCaptor.forClass(RaftMessage.class);
        verify(raftNode1, times(2)).receive(fromPeers.capture(), receivedMessages.capture());
        assertEquals(fromPeers.getAllValues(), Arrays.asList("V2", "V2"));
        assertEquals(receivedMessages.getAllValues(), testMessages);
    }

    @Test
    public void testDrop() {
        when(raftNode1.id()).thenReturn("V1");

        when(raftNode2.id()).thenReturn("V2");
        when(raftNode2.isStarted()).thenReturn(true);

        IRaftNode.IRaftMessageSender raftMessageListenerV1 = raftNodeNetwork.connect(raftNode1);
        raftNodeNetwork.connect(raftNode2);
        raftNodeNetwork.drop("V1", "V2", 0.5f);
        int count = 1000;
        List<RaftMessage> sendMessages = new ArrayList<>();
        while (count-- > 0) {
            sendMessages.add(RaftMessage.newBuilder().setTerm(1).build());
        }
        raftMessageListenerV1.send(new HashMap<String, List<RaftMessage>>() {{
            put("V2", sendMessages);
        }});

        raftNodeNetwork.tick();


        ArgumentCaptor<String> fromPeers = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<RaftMessage> receivedMessages = ArgumentCaptor.forClass(RaftMessage.class);
        verify(raftNode2, atLeastOnce()).receive(fromPeers.capture(), receivedMessages.capture());
        assertTrue(receivedMessages.getAllValues().size() > 0);
        assertTrue(receivedMessages.getAllValues().size() * 1.0 / sendMessages.size() < 0.6);
        assertTrue(receivedMessages.getAllValues().size() * 1.0 / sendMessages.size() > 0.4);
    }

    @Test
    public void testDuplicate() {
        when(raftNode1.id()).thenReturn("V1");

        when(raftNode2.id()).thenReturn("V2");
        when(raftNode2.isStarted()).thenReturn(true);

        IRaftNode.IRaftMessageSender raftMessageListenerV1 = raftNodeNetwork.connect(raftNode1);
        raftNodeNetwork.connect(raftNode2);
        raftNodeNetwork.duplicate("V1", "V2", 0.5f);
        int count = 100;
        List<RaftMessage> sendMessages = new ArrayList<>();
        while (count-- > 0) {
            sendMessages.add(RaftMessage.newBuilder().setTerm(1).build());
        }
        raftMessageListenerV1.send(new HashMap<String, List<RaftMessage>>() {{
            put("V2", sendMessages);
        }});

        raftNodeNetwork.tick();

        ArgumentCaptor<String> fromPeers = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<RaftMessage> receivedMessages = ArgumentCaptor.forClass(RaftMessage.class);
        verify(raftNode2, atLeastOnce()).receive(fromPeers.capture(), receivedMessages.capture());
        assertTrue(receivedMessages.getAllValues().size() > sendMessages.size());
        assertTrue(receivedMessages.getAllValues().size() * 1.0 / sendMessages.size() > 1.3);
        assertTrue(receivedMessages.getAllValues().size() * 1.0 / sendMessages.size() < 1.7);
    }

    @Test
    public void testReorder() {
        when(raftNode1.id()).thenReturn("V1");

        when(raftNode2.id()).thenReturn("V2");
        when(raftNode2.isStarted()).thenReturn(true);

        IRaftNode.IRaftMessageSender raftMessageListenerV1 = raftNodeNetwork.connect(raftNode1);
        raftNodeNetwork.connect(raftNode2);
        raftNodeNetwork.reorder("V1", "V2", 0.5f);
        int count = 100;
        List<RaftMessage> sendMessages = new ArrayList<>();
        while (count > 0) {
            sendMessages.add(RaftMessage.newBuilder().setTerm(count).build());
            count--;
        }
        raftMessageListenerV1.send(new HashMap<String, List<RaftMessage>>() {{
            put("V2", sendMessages);
        }});

        raftNodeNetwork.tick();

        ArgumentCaptor<String> fromPeers = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<RaftMessage> receivedMessages = ArgumentCaptor.forClass(RaftMessage.class);
        verify(raftNode2, atLeastOnce()).receive(fromPeers.capture(), receivedMessages.capture());
        assertEquals(receivedMessages.getAllValues().size(), sendMessages.size());
        assertNotEquals(receivedMessages.getAllValues(), sendMessages);
    }

    @Test
    public void testDelay() {
        when(raftNode1.id()).thenReturn("V1");

        when(raftNode2.id()).thenReturn("V2");
        when(raftNode2.isStarted()).thenReturn(true);

        IRaftNode.IRaftMessageSender raftMessageListenerV1 = raftNodeNetwork.connect(raftNode1);
        raftNodeNetwork.connect(raftNode2);
        raftNodeNetwork.delay("V1", "V2", 3);
        raftMessageListenerV1.send(new HashMap<String, List<RaftMessage>>() {{
            put("V2", Arrays.asList(RaftMessage.newBuilder().setTerm(1).build()));
        }});

        raftNodeNetwork.tick();

        verify(raftNode2, times(0)).receive(anyString(), any(RaftMessage.class));

        raftNodeNetwork.tick();
        raftNodeNetwork.tick();
        raftNodeNetwork.tick();

        verify(raftNode2, times(1)).receive(anyString(), any(RaftMessage.class));
    }

    @Test
    public void testCut() {
        when(raftNode1.id()).thenReturn("V1");

        when(raftNode2.id()).thenReturn("V2");
        when(raftNode2.isStarted()).thenReturn(true);

        when(raftNode3.id()).thenReturn("V3");

        IRaftNode.IRaftMessageSender raftMessageListenerV1 = raftNodeNetwork.connect(raftNode1);
        IRaftNode.IRaftMessageSender raftMessageListenerV2 = raftNodeNetwork.connect(raftNode2);
        raftNodeNetwork.connect(raftNode3);
        raftNodeNetwork.cut("V1", "V3");
        List<RaftMessage> testMessages =
            Arrays.asList(RaftMessage.newBuilder().setTerm(1).build(), RaftMessage.newBuilder().setTerm(2).build());
        raftMessageListenerV1.send(new HashMap<String, List<RaftMessage>>() {{
            put("V2", testMessages);
            put("V3", testMessages);
        }});
        raftNodeNetwork.tick();

        ArgumentCaptor<String> fromPeers = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<RaftMessage> receivedMessages = ArgumentCaptor.forClass(RaftMessage.class);
        verify(raftNode2, times(2)).receive(fromPeers.capture(), receivedMessages.capture());
        assertEquals(fromPeers.getAllValues(), Arrays.asList("V1", "V1"));
        assertEquals(receivedMessages.getAllValues(), testMessages);

        verify(raftNode3, times(0)).receive(anyString(), any(RaftMessage.class));
    }

    @Test
    public void testIgnore() {
        when(raftNode1.id()).thenReturn("V1");

        when(raftNode2.id()).thenReturn("V2");
        when(raftNode2.isStarted()).thenReturn(true);

        when(raftNode3.id()).thenReturn("V3");

        IRaftNode.IRaftMessageSender raftMessageListenerV1 = raftNodeNetwork.connect(raftNode1);
        raftNodeNetwork.connect(raftNode2);
        raftNodeNetwork.connect(raftNode3);
        raftNodeNetwork.ignore("V1", "V3", RaftMessage.MessageTypeCase.APPENDENTRIES);
        List<RaftMessage> testMessages = Arrays.asList(
            RaftMessage.newBuilder().setTerm(1).setAppendEntries(AppendEntries.newBuilder().build()).build(),
            RaftMessage.newBuilder().setTerm(2).setAppendEntries(AppendEntries.newBuilder().build()).build());
        raftMessageListenerV1.send(new HashMap<String, List<RaftMessage>>() {{
            put("V2", testMessages);
            put("V3", testMessages);
        }});
        raftNodeNetwork.tick();

        ArgumentCaptor<String> fromPeers = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<RaftMessage> receivedMessages = ArgumentCaptor.forClass(RaftMessage.class);
        verify(raftNode2, times(2)).receive(fromPeers.capture(), receivedMessages.capture());
        assertEquals(fromPeers.getAllValues(), Arrays.asList("V1", "V1"));
        assertEquals(receivedMessages.getAllValues(), testMessages);

        verify(raftNode3, times(0)).receive(anyString(), any(RaftMessage.class));
    }

    @Test
    public void testIsolate() {
        when(raftNode1.id()).thenReturn("V1");
        when(raftNode2.id()).thenReturn("V2");
        when(raftNode3.id()).thenReturn("V3");

        IRaftNode.IRaftMessageSender raftMessageListenerV1 = raftNodeNetwork.connect(raftNode1);
        IRaftNode.IRaftMessageSender raftMessageListenerV2 = raftNodeNetwork.connect(raftNode2);
        raftNodeNetwork.connect(raftNode3);
        raftNodeNetwork.isolate("V1");
        List<RaftMessage> testMessages =
            Arrays.asList(RaftMessage.newBuilder().setTerm(1).build(), RaftMessage.newBuilder().setTerm(2).build());
        raftMessageListenerV1.send(new HashMap<String, List<RaftMessage>>() {{
            put("V2", testMessages);
            put("V3", testMessages);
        }});
        raftNodeNetwork.tick();

        verify(raftNode2, times(0)).receive(anyString(), any(RaftMessage.class));
        verify(raftNode3, times(0)).receive(anyString(), any(RaftMessage.class));

        raftMessageListenerV2.send(new HashMap<String, List<RaftMessage>>() {{
            put("V1", testMessages);
        }});
        raftNodeNetwork.tick();

        verify(raftNode1, times(0)).receive(anyString(), any(RaftMessage.class));
    }

    @Test
    public void testReset() {
        when(raftNode1.id()).thenReturn("V1");
        when(raftNode2.id()).thenReturn("V2");
        when(raftNode2.isStarted()).thenReturn(true);

        IRaftNode.IRaftMessageSender raftMessageListenerV1 = raftNodeNetwork.connect(raftNode1);
        raftNodeNetwork.connect(raftNode2);
        raftNodeNetwork.cut("V1", "V2");
        int count = 100;
        List<RaftMessage> sendMessages = new ArrayList<>();
        while (count-- > 0) {
            sendMessages.add(RaftMessage.newBuilder().setTerm(1).build());
        }
        raftMessageListenerV1.send(new HashMap<String, List<RaftMessage>>() {{
            put("V2", sendMessages);
        }});

        raftNodeNetwork.tick();

        verify(raftNode2, times(0)).receive(anyString(), any(RaftMessage.class));

        raftNodeNetwork.recover();

        raftMessageListenerV1.send(new HashMap<String, List<RaftMessage>>() {{
            put("V2", sendMessages);
        }});

        raftNodeNetwork.tick();

        ArgumentCaptor<String> fromPeers = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<RaftMessage> receivedMessages = ArgumentCaptor.forClass(RaftMessage.class);
        verify(raftNode2, atLeastOnce()).receive(fromPeers.capture(), receivedMessages.capture());
        assertEquals(receivedMessages.getAllValues().size(), sendMessages.size());
        assertEquals(receivedMessages.getAllValues(), sendMessages);
    }
}
