/*
 * Copyright (c) 2023. The BifroMQ Authors. All Rights Reserved.
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

package org.apache.bifromq.plugin.eventcollector.mqttbroker.channelclosed;

import org.apache.bifromq.plugin.eventcollector.Event;
import java.net.InetSocketAddress;
import lombok.ToString;

/**
 * ChannelClosed event will be reported during session establishment process, especially before client being identified.
 * Examine the reason enum and downcast to corresponding subclass for concrete details
 */
@ToString(callSuper = true)
public abstract class ChannelClosedEvent<T extends ChannelClosedEvent<T>> extends Event<T> {
    protected InetSocketAddress peerAddress;

    @Override
    public void clone(T orig) {
        this.peerAddress = orig.peerAddress;
    }

    public final InetSocketAddress peerAddress() {
        return this.peerAddress;
    }

    public final T peerAddress(InetSocketAddress peerAddress) {
        this.peerAddress = peerAddress;
        return (T) this;
    }
}
