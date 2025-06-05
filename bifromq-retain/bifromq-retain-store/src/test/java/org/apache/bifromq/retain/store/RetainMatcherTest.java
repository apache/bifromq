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

package org.apache.bifromq.retain.store;

import static org.apache.bifromq.retain.store.RetainMatcher.MatchResult.MATCHED_AND_CONTINUE;
import static org.apache.bifromq.retain.store.RetainMatcher.MatchResult.MATCHED_AND_STOP;
import static org.apache.bifromq.retain.store.RetainMatcher.MatchResult.MISMATCH_AND_CONTINUE;
import static org.apache.bifromq.retain.store.RetainMatcher.MatchResult.MISMATCH_AND_STOP;
import static org.apache.bifromq.retain.store.RetainMatcher.match;
import static org.apache.bifromq.util.TopicUtil.parse;
import static org.testng.Assert.assertEquals;

import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.Test;

@Slf4j
public class RetainMatcherTest {
    @Test
    public void matches() {
        assertEquals(matches("/", "/"), MATCHED_AND_STOP);
        assertEquals(matches("/", "+/+"), MATCHED_AND_CONTINUE);
        assertEquals(matches("/", "+/#"), MATCHED_AND_CONTINUE);
        assertEquals(matches("/", "#"), MATCHED_AND_CONTINUE);
        assertEquals(matches("//", "//"), MATCHED_AND_STOP);
        assertEquals(matches("//", "#"), MATCHED_AND_CONTINUE);
        assertEquals(matches("//", "+/#"), MATCHED_AND_CONTINUE);
        assertEquals(matches("a", "a"), MATCHED_AND_STOP);
        assertEquals(matches("a", "a/#"), MATCHED_AND_CONTINUE);
        assertEquals(matches("a", "#"), MATCHED_AND_CONTINUE);
        assertEquals(matches("a", "/#"), MATCHED_AND_CONTINUE);
        assertEquals(matches("a", "+"), MATCHED_AND_CONTINUE);
        assertEquals(matches("a/", "#"), MATCHED_AND_CONTINUE);
        assertEquals(matches("a/", "+/#"), MATCHED_AND_CONTINUE);
        assertEquals(matches("a/", "a/"), MATCHED_AND_STOP);
        assertEquals(matches("/a", "/a"), MATCHED_AND_STOP);
        assertEquals(matches("/a", "/a/#"), MATCHED_AND_CONTINUE);
        assertEquals(matches("/a", "/+"), MATCHED_AND_CONTINUE);
        assertEquals(matches("/a/b/c", "/+/#"), MATCHED_AND_CONTINUE);
        assertEquals(matches("/a/b/c", "/#"), MATCHED_AND_CONTINUE);
        assertEquals(matches("/a/b/c", "#"), MATCHED_AND_CONTINUE);
        assertEquals(matches("/a/b/c", "/a/b/c/#"), MATCHED_AND_CONTINUE);
        assertEquals(matches("/a/b/c", "/a/b/#"), MATCHED_AND_CONTINUE);
        assertEquals(matches("a/b/c", "#"), MATCHED_AND_CONTINUE);
        assertEquals(matches("a/b/c", "a/#"), MATCHED_AND_CONTINUE);
        assertEquals(matches("a/b/c", "b/#"), MISMATCH_AND_CONTINUE);
        assertEquals(matches("a/b/c", "0/#"), MISMATCH_AND_CONTINUE);
        assertEquals(matches("a/b/c", "a/c/#"), MISMATCH_AND_CONTINUE);
        assertEquals(matches("a/b/c", "a/b/#"), MATCHED_AND_CONTINUE);
        assertEquals(matches("a/b/c", "a/+/c"), MATCHED_AND_CONTINUE);

        assertEquals(matches("a", "/a"), MISMATCH_AND_STOP);
        assertEquals(matches("a", "+/"), MISMATCH_AND_STOP);
        assertEquals(matches("a/", "/a"), MISMATCH_AND_STOP);
        assertEquals(matches("a/b", "+/a"), MISMATCH_AND_STOP);
        assertEquals(matches("a/b", "+/d"), MISMATCH_AND_CONTINUE);

        assertEquals(matches("a/b/c", "a/+/d"), MISMATCH_AND_CONTINUE);
        assertEquals(matches("a/b/c", "+/a/c"), MISMATCH_AND_STOP);
        assertEquals(matches("a/b/c", "+/a/c"), MISMATCH_AND_STOP);
        assertEquals(matches("a/b/c", "+/c/c"), MISMATCH_AND_CONTINUE);
        assertEquals(matches("a/b/c", "a/b/d"), MISMATCH_AND_STOP);
    }

    private RetainMatcher.MatchResult matches(String topic, String topicFilter) {
        return match(parse(topic, false), parse(topicFilter, false));
    }
}
