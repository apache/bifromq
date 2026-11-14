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

package org.apache.bifromq.basecrdt.store;

import static com.google.protobuf.ByteString.copyFromUtf8;
import static org.testng.Assert.assertTrue;

import io.reactivex.rxjava3.subjects.PublishSubject;
import io.reactivex.rxjava3.subjects.Subject;
import org.apache.bifromq.basecrdt.core.api.CRDTURI;
import org.apache.bifromq.basecrdt.core.api.CausalCRDTType;
import org.apache.bifromq.basecrdt.proto.Replica;
import org.apache.bifromq.basecrdt.store.proto.CRDTStoreMessage;
import org.testng.annotations.Test;

public class CRDTStoreSubscriptionTest {

    @Test
    public void malformedMessageKeepsInboundSubscriptionAlive() {
        ICRDTStore store = ICRDTStore.newInstance(CRDTStoreOptions.builder().id("store").build());
        Subject<CRDTStoreMessage> receiver = PublishSubject.create();
        try {
            store.start(receiver);
            String uri = CRDTURI.toURI(CausalCRDTType.ormap, "replica");
            Replica replica = ReplicaIdGenerator.generate(uri, copyFromUtf8("store"));
            store.host(replica, replica.getId());
            assertTrue(receiver.hasObservers());

            CRDTStoreMessage malformed = CRDTStoreMessage.newBuilder()
                .setUri(uri)
                .setSender(copyFromUtf8("peer"))
                .setReceiver(replica.getId())
                .setPayload(copyFromUtf8("not a gzip payload"))
                .build();
            receiver.onNext(malformed);

            assertTrue(receiver.hasObservers(), "malformed message must not dispose the subscription");
        } finally {
            store.stop();
        }
    }
}
