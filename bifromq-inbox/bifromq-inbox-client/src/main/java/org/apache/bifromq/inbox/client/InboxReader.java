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

package org.apache.bifromq.inbox.client;

import org.apache.bifromq.basehlc.HLC;
import org.apache.bifromq.inbox.storage.proto.Fetched;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class InboxReader implements IInboxClient.IInboxReader {
    private final String inboxId;
    private final long incarnation;
    private final InboxFetchPipeline ppln;
    private final long sessionId = HLC.INST.get();
    private volatile long lastFetchQoS0Seq = -1;
    private volatile long lastFetchSendBufferSeq = -1;

    public InboxReader(String inboxId,
                       long incarnation,
                       InboxFetchPipeline ppln) {
        this.inboxId = inboxId;
        this.incarnation = incarnation;
        this.ppln = ppln;
    }

    @Override
    public void fetch(Consumer<Fetched> consumer) {
        ppln.fetch(sessionId, inboxId, incarnation, (fetched) -> {
            if (fetched.getResult() == Fetched.Result.OK) {
                if (fetched.getQos0MsgCount() > 0) {
                    lastFetchQoS0Seq = fetched.getQos0Msg(fetched.getQos0MsgCount() - 1).getSeq();
                }
                if (fetched.getSendBufferMsgCount() > 0) {
                    lastFetchSendBufferSeq = fetched.getSendBufferMsg(fetched.getSendBufferMsgCount() - 1).getSeq();
                }
            }
            consumer.accept(fetched);
        });
    }

    @Override
    public void hint(int bufferCapacity) {
        try {
            ppln.hint(sessionId, inboxId, incarnation, bufferCapacity, lastFetchQoS0Seq, lastFetchSendBufferSeq);
        } catch (Throwable e) {
            log.warn("Failed to send hint: inboxId={}", inboxId, e);
        }
    }

    @Override
    public void close() {
        // tell server side to remove FetchState
        hint(-1);
        ppln.stopFetch(sessionId, inboxId, incarnation);
    }
}
