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

package org.apache.bifromq.mqtt.handler;

import io.netty.channel.Channel;
import java.util.function.IntConsumer;
import java.util.function.IntSupplier;

public final class SendBufferCapacityHinter {
    private static final IntConsumer DUMMY = i -> {
    };
    private final Channel channel;
    private final IntSupplier unconfirmedSizer;
    private final float triggerRatio;
    private IntConsumer hintConsumer = DUMMY;
    private int lastHint = -1;
    private int lastHintRemaining = -1;
    private int lastMsgSize;

    public SendBufferCapacityHinter(Channel channel, IntSupplier unconfirmedSizer, float triggerRatio) {
        assert triggerRatio > 0;
        this.channel = channel;
        this.unconfirmedSizer = unconfirmedSizer;
        this.triggerRatio = Math.max(triggerRatio, 0.8f);
    }

    public void reset() {
        lastHint = -1;
        lastHintRemaining = -1;
        lastMsgSize = 0;
        sendHint(100);
    }

    // callback to receive hint update
    public void hint(IntConsumer hintConsumer) {
        this.hintConsumer = hintConsumer;
        sendHint(100);
    }

    public boolean hasCapacity() {
        return lastHint == -1 || lastHintRemaining > 0;
    }

    // called when channel's writability changed
    public void onWritabilityChanged() {
        if (channel.isWritable() && lastMsgSize > 0) {
            // at least one must be ensured
            sendHint(Math.max(1, estimateCapacity(lastMsgSize)));
        }
    }

    // called after one message written to send buffer. msgSize is the size of the message just written
    public void onOneMessageBuffered(int msgSize) {
        if (lastHint == -1) {
            sendHint(Math.max(1, estimateCapacity(msgSize)));
        } else if (!channel.isWritable()) {
            if (lastHint > 0) {
                sendHint(0);
            }
        } else {
            lastHintRemaining = Math.max(0, lastHintRemaining - 1);
            if (remainingRatio() <= triggerRatio) {
                sendHint(Math.max(1, estimateCapacity(msgSize)));
            }
        }
        lastMsgSize = msgSize; // for simplicity, always use recent msgSize to predict capacity
    }

    // called when one message has been confirmed:
    // for qos0 msg, it's confirmed after being sent
    // for qos1 msg, it's confirmed when received PubAck or exceeded max resent times
    // for qos2 msg, it's confirmed when received PubComp or exceeded max resent times
    public void onConfirm() {
        if (remainingRatio() <= triggerRatio && lastMsgSize > 0) {
            int estCap = estimateCapacity(lastMsgSize);
            if (estCap > 0) {
                sendHint(estCap);
            }
        }
    }

    private void sendHint(int estCap) {
        lastHint = estCap;
        lastHintRemaining = estCap;
        hintConsumer.accept(estCap);
    }

    private float remainingRatio() {
        return ((float) lastHintRemaining) / lastHint;
    }

    private int estimateCapacity(int estMsgSize) {
        int estBufferSize = (int) channel.bytesBeforeUnwritable() / estMsgSize;
        return Math.max(estBufferSize - unconfirmedSizer.getAsInt(), 0);
    }
}
