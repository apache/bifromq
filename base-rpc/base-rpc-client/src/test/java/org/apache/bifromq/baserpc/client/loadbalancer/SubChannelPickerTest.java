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

package org.apache.bifromq.baserpc.client.loadbalancer;

import static org.apache.bifromq.baserpc.client.exception.ExceptionUtil.SERVER_NOT_FOUND;
import static org.apache.bifromq.baserpc.client.exception.ExceptionUtil.SERVER_UNREACHABLE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import io.grpc.Attributes;
import io.grpc.CallOptions;
import io.grpc.ConnectivityState;
import io.grpc.ConnectivityStateInfo;
import io.grpc.EquivalentAddressGroup;
import io.grpc.LoadBalancer;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.testng.annotations.Test;

public class SubChannelPickerTest {
    private static final MethodDescriptor<Void, Void> TEST_METHOD =
        MethodDescriptor.<Void, Void>newBuilder()
            .setType(MethodDescriptor.MethodType.UNARY)
            .setFullMethodName(MethodDescriptor.generateFullMethodName("service", "method"))
            .setRequestMarshaller(new VoidMarshaller())
            .setResponseMarshaller(new VoidMarshaller())
            .build();

    @Test
    public void pickReadySubchannel() {
        SubChannelPicker picker = new SubChannelPicker();
        ChannelList channelList = new ChannelList(false);
        LoadBalancer.Subchannel readyChannel = new TestSubchannel(readyAttributes());
        channelList.subChannels.add(readyChannel);
        picker.refresh(Map.of("serverA", channelList));

        Metadata headers = new Metadata();
        headers.put(Constants.DESIRED_SERVER_META_KEY, "serverA");
        LoadBalancer.PickResult result = picker.pickSubchannel(new TestPickArgs(headers));

        assertSame(result.getSubchannel(), readyChannel);
        assertTrue(result.getStatus().isOk());
        assertFalse(result.isDrop());
    }

    @Test
    public void pickReturnsUnavailableWhenNoReadyChannel() {
        SubChannelPicker picker = new SubChannelPicker();
        ChannelList channelList = new ChannelList(false);
        channelList.subChannels.add(new TestSubchannel(connectingAttributes()));
        picker.refresh(Map.of("serverA", channelList));

        Metadata headers = new Metadata();
        headers.put(Constants.DESIRED_SERVER_META_KEY, "serverA");
        LoadBalancer.PickResult result = picker.pickSubchannel(new TestPickArgs(headers));

        assertNull(result.getSubchannel());
        assertEquals(result.getStatus(), SERVER_UNREACHABLE);
        assertFalse(result.isDrop());
    }

    @Test
    public void pickReturnsNotFoundWhenServerMissing() {
        SubChannelPicker picker = new SubChannelPicker();
        picker.refresh(Collections.emptyMap());

        Metadata headers = new Metadata();
        headers.put(Constants.DESIRED_SERVER_META_KEY, "serverA");
        LoadBalancer.PickResult result = picker.pickSubchannel(new TestPickArgs(headers));

        assertNull(result.getSubchannel());
        assertEquals(result.getStatus(), SERVER_NOT_FOUND);
        assertFalse(result.isDrop());
    }

    private static Attributes readyAttributes() {
        return Attributes.newBuilder()
            .set(Constants.STATE_INFO,
                new AtomicReference<>(ConnectivityStateInfo.forNonError(ConnectivityState.READY)))
            .build();
    }

    private static Attributes connectingAttributes() {
        return Attributes.newBuilder()
            .set(Constants.STATE_INFO,
                new AtomicReference<>(ConnectivityStateInfo.forNonError(ConnectivityState.CONNECTING)))
            .build();
    }

    private static class TestPickArgs extends LoadBalancer.PickSubchannelArgs {
        private final Metadata headers;

        TestPickArgs(Metadata headers) {
            this.headers = headers;
        }

        @Override
        public CallOptions getCallOptions() {
            return CallOptions.DEFAULT;
        }

        @Override
        public Metadata getHeaders() {
            return headers;
        }

        @Override
        public MethodDescriptor<?, ?> getMethodDescriptor() {
            return TEST_METHOD;
        }
    }

    private static class TestSubchannel extends LoadBalancer.Subchannel {
        private final Attributes attributes;

        TestSubchannel(Attributes attributes) {
            this.attributes = attributes;
        }

        @Override
        public void shutdown() {
        }

        @Override
        public void requestConnection() {
        }

        @Override
        public List<EquivalentAddressGroup> getAllAddresses() {
            return Collections.emptyList();
        }

        @Override
        public Attributes getAttributes() {
            return attributes;
        }
    }

    private static class VoidMarshaller implements MethodDescriptor.Marshaller<Void> {
        @Override
        public InputStream stream(Void value) {
            return new ByteArrayInputStream(new byte[0]);
        }

        @Override
        public Void parse(InputStream stream) {
            return null;
        }
    }
}
