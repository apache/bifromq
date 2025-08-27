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

import static org.awaitility.Awaitility.await;

import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.schedulers.Schedulers;
import io.reactivex.rxjava3.subjects.PublishSubject;
import io.reactivex.rxjava3.subjects.Subject;
import java.time.Duration;
import java.util.Collections;
import org.apache.bifromq.basecrdt.core.api.CRDTURI;
import org.apache.bifromq.basecrdt.core.api.CausalCRDTType;
import org.apache.bifromq.basecrdt.core.api.IMVReg;
import org.apache.bifromq.basecrdt.core.api.IORMap;
import org.apache.bifromq.basecrdt.core.api.MVRegOperation;
import org.apache.bifromq.basecrdt.core.api.ORMapOperation;
import org.apache.bifromq.basecrdt.proto.Replica;
import org.apache.bifromq.basecrdt.store.compressor.GzipCompressor;
import org.apache.bifromq.basecrdt.store.proto.CRDTStoreMessage;
import org.apache.bifromq.basecrdt.store.proto.MessagePayload;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

public class AntiEntropyResilienceTest {
    private ICRDTStore storeA;
    private ICRDTStore storeB;
    private Subject<CRDTStoreMessage> chAB;
    private Subject<CRDTStoreMessage> chBA;

    @AfterMethod(alwaysRun = true)
    public void teardown() {
        if (storeA != null) {
            storeA.stop();
            storeA = null;
        }
        if (storeB != null) {
            storeB.stop();
            storeB = null;
        }
    }

    @Test(groups = "integration")
    public void testConvergeWithDroppedAckOnce() {
        CRDTStoreOptions optsA = CRDTStoreOptions.builder()
            .inflationInterval(Duration.ofMillis(50))
            .maxEventsInDelta(16)
            .build();
        CRDTStoreOptions optsB = CRDTStoreOptions.builder()
            .inflationInterval(Duration.ofMillis(50))
            .maxEventsInDelta(16)
            .build();
        storeA = ICRDTStore.newInstance(optsA);
        storeB = ICRDTStore.newInstance(optsB);

        chAB = PublishSubject.<CRDTStoreMessage>create().toSerialized();
        chBA = PublishSubject.<CRDTStoreMessage>create().toSerialized();

        // Interpose B->A path to drop the first ACK intentionally to exercise resend/late-ack path
        GzipCompressor compressor = new GzipCompressor();
        final boolean[] firstAckDropped = {false};

        // Start stores with the interposed channels
        storeA.start(chBA);
        storeB.start(chAB
            .flatMap(msg -> {
                // inspect payload; if it's ACK and first time, drop it once
                MessagePayload payload = MessagePayloadUtil.decompress(compressor, msg);
                if (payload.getMsgTypeCase() == MessagePayload.MsgTypeCase.ACK && !firstAckDropped[0]) {
                    firstAckDropped[0] = true;
                    // drop this ack
                    return Observable.empty();
                }
                return Observable.just(msg);
            }));

        storeA.storeMessages()
            .observeOn(Schedulers.single())
            .subscribe(chAB::onNext);
        storeB.storeMessages()
            .observeOn(Schedulers.single())
            .subscribe(chBA::onNext);

        String uri = CRDTURI.toURI(CausalCRDTType.ormap, "test");
        // Build replicas
        Replica rA = ReplicaIdGenerator.generate(uri);
        Replica rB = ReplicaIdGenerator.generate(uri);
        ByteString addrA = ByteString.copyFromUtf8("A");
        ByteString addrB = ByteString.copyFromUtf8("B");

        // Host replicas
        IORMap ormapA = storeA.host(rA, addrA);
        IORMap ormapB = storeB.host(rB, addrB);

        // Join neighbors
        storeA.join(rA, Collections.singleton(addrB));
        storeB.join(rB, Collections.singleton(addrA));

        // Write a value from A
        ByteString key = ByteString.copyFromUtf8("k");
        ByteString val = ByteString.copyFromUtf8("v1");
        ormapA.execute(ORMapOperation.update(key).with(MVRegOperation.write(val))).join();

        await().until(() -> {
            IMVReg regB = ormapB.getMVReg(key);
            ByteString read = Sets.newHashSet(regB.read()).stream().findFirst().orElse(ByteString.EMPTY);
            return val.equals(read);
        });
    }

    @Test(groups = "integration")
    public void testConvergeWithLateUnmatchedAck() {
        CRDTStoreOptions optsC = CRDTStoreOptions.builder()
            .inflationInterval(Duration.ofMillis(50))
            .maxEventsInDelta(16)
            .build();
        CRDTStoreOptions optsD = CRDTStoreOptions.builder()
            .inflationInterval(Duration.ofMillis(50))
            .maxEventsInDelta(16)
            .build();
        ICRDTStore storeC = ICRDTStore.newInstance(optsC);
        ICRDTStore storeD = ICRDTStore.newInstance(optsD);

        Subject<CRDTStoreMessage> cToD = PublishSubject.<CRDTStoreMessage>create().toSerialized();
        Subject<CRDTStoreMessage> dToC = PublishSubject.<CRDTStoreMessage>create().toSerialized();

        GzipCompressor compressor = new GzipCompressor();
        final CRDTStoreMessage[] delayedAck = {null};
        final int[] deltaCountFromC = {0};

        // Wire inbound with logic: buffer first ACK from D->C, only deliver after second DELTA from C
        storeC.start(dToC
            .flatMap(msg -> {
                MessagePayload payload = MessagePayloadUtil.decompress(compressor, msg);
                if (payload.getMsgTypeCase() == MessagePayload.MsgTypeCase.ACK && delayedAck[0] == null) {
                    delayedAck[0] = msg; // buffer first ACK
                    return Observable.empty();
                }
                return Observable.just(msg);
            }));
        storeD.start(cToD
            .flatMap(msg -> {
                MessagePayload payload = MessagePayloadUtil.decompress(compressor, msg);
                if (payload.getMsgTypeCase() == MessagePayload.MsgTypeCase.DELTA) {
                    deltaCountFromC[0]++;
                    if (deltaCountFromC[0] >= 2 && delayedAck[0] != null) {
                        CRDTStoreMessage ack = delayedAck[0];
                        delayedAck[0] = null;
                        dToC.onNext(ack);
                    }
                }
                return Observable.just(msg);
            }));

        storeC.storeMessages().observeOn(Schedulers.single()).subscribe(cToD::onNext);
        storeD.storeMessages().observeOn(Schedulers.single()).subscribe(dToC::onNext);

        // Host replicas
        String uri = CRDTURI.toURI(CausalCRDTType.ormap, "test-late-ack");
        Replica rC = ReplicaIdGenerator.generate(uri);
        Replica rD = ReplicaIdGenerator.generate(uri);
        ByteString addrC = ByteString.copyFromUtf8("C");
        ByteString addrD = ByteString.copyFromUtf8("D");
        IORMap ormapC = storeC.host(rC, addrC);
        IORMap ormapD = storeD.host(rD, addrD);
        storeC.join(rC, Collections.singleton(addrD));
        storeD.join(rD, Collections.singleton(addrC));

        // Write on C
        ByteString key = ByteString.copyFromUtf8("k2");
        ByteString val = ByteString.copyFromUtf8("v2");
        ormapC.execute(ORMapOperation.update(key).with(MVRegOperation.write(val))).join();

        // Await convergence on D even though first ACK is delivered late and unmatched
        await().until(() -> {
            IMVReg regD = ormapD.getMVReg(key);
            ByteString read = Sets.newHashSet(regD.read()).stream().findFirst().orElse(ByteString.EMPTY);
            return val.equals(read);
        });

        storeC.stop();
        storeD.stop();
    }
}
