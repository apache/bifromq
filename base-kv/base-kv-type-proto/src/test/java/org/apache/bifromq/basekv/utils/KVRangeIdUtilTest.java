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

package org.apache.bifromq.basekv.utils;

import static org.testng.Assert.assertTrue;

import org.apache.bifromq.basekv.proto.KVRangeId;
import lombok.SneakyThrows;
import org.testng.annotations.Test;

public class KVRangeIdUtilTest {

    @SneakyThrows
    @Test
    public void testGenerate() {
        KVRangeId id1 = KVRangeIdUtil.generate();
        KVRangeId id2 = KVRangeIdUtil.next(id1);
        Thread.sleep(1);
        KVRangeId id3 = KVRangeIdUtil.generate();

        assertTrue(id1.getEpoch() == id2.getEpoch() && id1.getId() < id2.getId());
        assertTrue(id2.getEpoch() < id3.getEpoch());
    }
}
