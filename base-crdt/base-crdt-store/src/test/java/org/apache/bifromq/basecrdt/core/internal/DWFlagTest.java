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

package org.apache.bifromq.basecrdt.core.internal;

import static org.apache.bifromq.basecrdt.core.api.CRDTURI.toURI;
import static org.apache.bifromq.basecrdt.core.api.CausalCRDTType.dwflag;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.apache.bifromq.basecrdt.core.api.DWFlagOperation;
import org.apache.bifromq.basecrdt.core.api.IDWFlag;
import org.apache.bifromq.basecrdt.proto.Replica;
import com.google.protobuf.ByteString;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.Test;

@Slf4j
public class DWFlagTest extends CRDTTest {
    private final Replica leftReplica = Replica.newBuilder()
        .setUri(toURI(dwflag, "dwflag"))
        .setId(ByteString.copyFromUtf8("left-address"))
        .build();
    private final Replica rightReplica = Replica.newBuilder()
        .setUri(toURI(dwflag, "dwflag"))
        .setId(ByteString.copyFromUtf8("right-address"))
        .build();

    @Test
    public void testOperation() {
        DWFlagInflater dwFlagInflater = new DWFlagInflater(leftReplica,
            newStateLattice(leftReplica, 1000), executor, Duration.ofMillis(100));
        IDWFlag dwFlag = dwFlagInflater.getCRDT();
        assertEquals(dwFlag.id(), leftReplica);

        assertTrue(dwFlag.read());
        dwFlag.execute(DWFlagOperation.disable()).join();
        assertFalse(dwFlag.read());

        dwFlag.execute(DWFlagOperation.enable());
        dwFlag.execute(DWFlagOperation.disable()).join();
        assertFalse(dwFlag.read());

        dwFlag.execute(DWFlagOperation.enable()).join();
        assertTrue(dwFlag.read());
    }

    @Test
    public void testJoin() {
        DWFlagInflater leftInflater = new DWFlagInflater(leftReplica,
            newStateLattice(leftReplica, 1000000),
            executor, Duration.ofMillis(100));
        IDWFlag left = leftInflater.getCRDT();

        DWFlagInflater rightInflater = new DWFlagInflater(rightReplica,
            newStateLattice(rightReplica, 1000000),
            executor, Duration.ofMillis(100));
        IDWFlag right = rightInflater.getCRDT();

        left.execute(DWFlagOperation.disable()).join();
        right.execute(DWFlagOperation.enable()).join();
        // nothing to send from right to left
        assertFalse(rightInflater
            .delta(leftInflater.latticeEvents(), leftInflater.historyEvents(), 10).join().isPresent());
        // something to send from left to right
        rightInflater.join(leftInflater
            .delta(rightInflater.latticeEvents(), rightInflater.historyEvents(), 10).join().get()).join();

        assertFalse(left.read());
        assertEquals(right.read(), left.read());

        right.execute(DWFlagOperation.enable()).join();
        assertTrue(right.read());

        sync(leftInflater, rightInflater);
        assertTrue(left.read());
        assertEquals(right.read(), left.read());
    }
}
