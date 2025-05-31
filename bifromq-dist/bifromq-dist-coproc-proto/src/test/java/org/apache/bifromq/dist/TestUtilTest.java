/*
 * Copyright (c) 2024. The BifroMQ Authors. All Rights Reserved.
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

package org.apache.bifromq.dist;

import static org.testng.Assert.assertEquals;

import com.google.common.collect.Lists;
import java.util.List;
import org.testng.annotations.Test;

public class TestUtilTest {
    @Test
    public void testExpand() {
        List<String> topicFilters = TestUtil.expand(TestUtil.randomTopic());
        List<String> copy = Lists.newArrayList(topicFilters);
        topicFilters.sort(String::compareTo);
        assertEquals(topicFilters, copy);
    }

    @Test
    public void testExpandSysTopic() {
        String topic = "$sys/a/b/c";
        List<String> topicFilters = TestUtil.expand(topic);
        List<String> copy = Lists.newArrayList(topicFilters);
        topicFilters.sort(String::compareTo);
        assertEquals(topicFilters, copy);
    }
}
