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

package org.apache.bifromq.basecrdt.core.internal;

import static org.apache.bifromq.basecrdt.core.api.CRDTURI.toURI;
import static org.apache.bifromq.basecrdt.core.api.CausalCRDTType.ewflag;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.apache.bifromq.basecrdt.core.api.EWFlagOperation;
import org.apache.bifromq.basecrdt.core.api.IEWFlag;
import org.apache.bifromq.basecrdt.proto.Replica;
import com.google.protobuf.ByteString;
import io.reactivex.rxjava3.observers.TestObserver;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.Test;

@Slf4j
public class EWFlagTest extends CRDTTest {
    private final Replica leftReplica = Replica.newBuilder()
        .setUri(toURI(ewflag, "ewflag"))
        .setId(ByteString.copyFromUtf8("left-address"))
        .build();
    private final Replica rightReplica = Replica.newBuilder()
        .setUri(toURI(ewflag, "ewflag"))
        .setId(ByteString.copyFromUtf8("right-address"))
        .build();

    @Test
    public void testOperation() {
        EWFlagInflater ewFlagInflater = new EWFlagInflater(leftReplica,
            newStateLattice(leftReplica, 1000),
            executor, Duration.ofMillis(100));
        IEWFlag ewFlag = ewFlagInflater.getCRDT();
        assertEquals(ewFlag.id(), leftReplica);

        assertFalse(ewFlag.read());
        ewFlag.execute(EWFlagOperation.enable()).join();
        assertTrue(ewFlag.read());

        ewFlag.execute(EWFlagOperation.disable());
        ewFlag.execute(EWFlagOperation.enable()).join();
        assertTrue(ewFlag.read());

        ewFlag.execute(EWFlagOperation.disable()).join();
        assertFalse(ewFlag.read());
    }

    @Test
    public void testJoin() {
        EWFlagInflater leftInflater = new EWFlagInflater(leftReplica,
            newStateLattice(leftReplica, 1000000), executor, Duration.ofMillis(100));
        IEWFlag left = leftInflater.getCRDT();

        EWFlagInflater rightInflater = new EWFlagInflater(rightReplica,
            newStateLattice(rightReplica, 1000000), executor, Duration.ofMillis(100));
        IEWFlag right = rightInflater.getCRDT();

        left.execute(EWFlagOperation.enable()).join();
        right.execute(EWFlagOperation.disable()).join();
        sync(leftInflater, rightInflater);

        assertTrue(left.read());
        assertEquals(right.read(), left.read());

        TestObserver<Long> inflationObserver = new TestObserver<>();
        right.inflation().subscribe(inflationObserver);

        right.execute(EWFlagOperation.disable()).join();
        assertFalse(inflationObserver.values().isEmpty());
        sync(leftInflater, rightInflater);
        assertFalse(left.read());
        assertEquals(right.read(), left.read());
    }
}
