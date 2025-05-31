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

package org.apache.bifromq.util;

import static org.apache.bifromq.util.TopicUtil.escape;
import static org.apache.bifromq.util.TopicUtil.from;
import static org.apache.bifromq.util.TopicUtil.isWildcardTopicFilter;
import static org.apache.bifromq.util.TopicUtil.parse;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.apache.bifromq.type.RouteMatcher;
import java.util.List;
import org.testng.annotations.Test;

public class TopicUtilsTest {
    @Test
    public void check() {
        assertTrue(isWildcardTopicFilter("#"));
        assertTrue(isWildcardTopicFilter("+"));
        assertTrue(isWildcardTopicFilter("/#"));
        assertTrue(isWildcardTopicFilter("/+"));
        assertFalse(isWildcardTopicFilter("/"));
    }

    @Test
    public void testParse() {
        assertEquals(parse("", false), List.of(""));
        assertEquals(parse(" ", false), List.of(" "));
        assertEquals(parse("/", false), List.of("", ""));
        assertEquals(parse(escape("/"), true), List.of("", ""));
        assertEquals(parse("//", false), List.of("", "", ""));
        assertEquals(parse(escape("//"), true), List.of("", "", ""));
        assertEquals(parse(" //", false), List.of(" ", "", ""));
        assertEquals(parse(escape(" //"), true), List.of(" ", "", ""));
        assertEquals(parse(" / / ", false), List.of(" ", " ", " "));
        assertEquals(parse(escape(" / / "), true), List.of(" ", " ", " "));
        assertEquals(parse("a/", false), List.of("a", ""));
        assertEquals(parse(escape("a/"), true), List.of("a", ""));
        assertEquals(parse("a/b", false), List.of("a", "b"));
        assertEquals(parse(escape("a/b"), true), List.of("a", "b"));
        assertEquals(parse("a/b/", false), List.of("a", "b", ""));
        assertEquals(parse(escape("a/b/"), true), List.of("a", "b", ""));
    }

    @Test
    public void testIsValidTopic() {
        assertTrue(TopicUtil.isValidTopic("/", 40, 16, 255));
        assertTrue(TopicUtil.isValidTopic("//", 40, 16, 255));
        assertFalse(TopicUtil.isValidTopic("", 40, 16, 255));
        assertTrue(TopicUtil.isValidTopic(" ", 40, 16, 255));
        assertTrue(TopicUtil.isValidTopic("/ ", 40, 16, 255));
        assertTrue(TopicUtil.isValidTopic("/ /", 40, 16, 255));
        assertFalse(TopicUtil.isValidTopic("/\u0000abc/", 40, 16, 255));
        assertFalse(TopicUtil.isValidTopic("/abc/", 2, 16, 255));
        assertTrue(TopicUtil.isValidTopic("abc", 4, 1, 255));
        assertFalse(TopicUtil.isValidTopic("/", 4, 1, 255));
        assertTrue(TopicUtil.isValidTopic("/", 4, 2, 255));
        assertFalse(TopicUtil.isValidTopic("/abcde/fghij", 5, 4, 10));
        assertFalse(TopicUtil.isValidTopic("/+/#", 5, 4, 10));

        assertFalse(TopicUtil.isValidTopic("$share/a/", 5, 4, 10));
        assertFalse(TopicUtil.isValidTopic("$share/a//", 5, 4, 10));

        assertTrue(TopicUtil.isValidTopic("$share", 10, 4, 20));
        assertTrue(TopicUtil.isValidTopic("$shared/a//", 10, 4, 20));
    }

    @Test
    public void testIsValidTopicFilter() {
        assertTrue(TopicUtil.isValidTopicFilter("/", 40, 16, 255));
        assertTrue(TopicUtil.isValidTopicFilter("//", 40, 16, 255));
        assertFalse(TopicUtil.isValidTopicFilter("", 40, 16, 255));
        assertTrue(TopicUtil.isValidTopicFilter(" ", 40, 16, 255));
        assertTrue(TopicUtil.isValidTopicFilter("/ ", 40, 16, 255));
        assertTrue(TopicUtil.isValidTopicFilter("/ /", 40, 16, 255));
        assertFalse(TopicUtil.isValidTopicFilter("/\u0000abc/", 40, 16, 255));
        assertFalse(TopicUtil.isValidTopicFilter("/abc/", 2, 16, 255));
        assertTrue(TopicUtil.isValidTopicFilter("abc", 4, 1, 255));
        assertFalse(TopicUtil.isValidTopicFilter("/", 4, 1, 255));
        assertTrue(TopicUtil.isValidTopicFilter("/", 4, 2, 255));
        assertFalse(TopicUtil.isValidTopicFilter("/abcde/fghij", 5, 4, 10));

        assertTrue(TopicUtil.isValidTopicFilter("#", 40, 16, 255));
        assertTrue(TopicUtil.isValidTopicFilter("a/#", 40, 16, 255));
        assertTrue(TopicUtil.isValidTopicFilter("+", 40, 16, 255));
        assertTrue(TopicUtil.isValidTopicFilter("+/", 40, 16, 255));
        assertTrue(TopicUtil.isValidTopicFilter("+/+", 40, 16, 255));
        assertTrue(TopicUtil.isValidTopicFilter("/+/#", 40, 16, 255));
        assertTrue(TopicUtil.isValidTopicFilter("+/a/#", 40, 16, 255));

        assertFalse(TopicUtil.isValidTopicFilter("#a", 40, 16, 255));
        assertFalse(TopicUtil.isValidTopicFilter("#/a", 40, 16, 255));
        assertFalse(TopicUtil.isValidTopicFilter("/a#", 40, 16, 255));
        assertFalse(TopicUtil.isValidTopicFilter("/a#a", 40, 16, 255));
        assertFalse(TopicUtil.isValidTopicFilter("/a+/", 40, 16, 255));
        assertFalse(TopicUtil.isValidTopicFilter("/+a/", 40, 16, 255));
        assertFalse(TopicUtil.isValidTopicFilter("/a+a/", 40, 16, 255));
        assertFalse(TopicUtil.isValidTopicFilter("a+", 40, 16, 255));
        assertFalse(TopicUtil.isValidTopicFilter("+a", 40, 16, 255));
        assertFalse(TopicUtil.isValidTopicFilter("/a/+#", 40, 16, 255));

        assertFalse(TopicUtil.isValidTopicFilter("$share/", 5, 4, 10));
        assertFalse(TopicUtil.isValidTopicFilter("$share/a", 5, 4, 10));
        assertFalse(TopicUtil.isValidTopicFilter("$share/\u0000/", 5, 4, 10));
        assertFalse(TopicUtil.isValidTopicFilter("$share//", 5, 4, 10));

        assertFalse(TopicUtil.isValidTopicFilter("$oshare/", 5, 4, 10));
        assertFalse(TopicUtil.isValidTopicFilter("$oshare//", 5, 4, 10));
        assertFalse(TopicUtil.isValidTopicFilter("$oshare/a", 5, 4, 10));
        assertFalse(TopicUtil.isValidTopicFilter("$oshare/\u0000/", 5, 4, 10));

        assertTrue(TopicUtil.isValidTopicFilter("$share", 10, 4, 100));
        assertTrue(TopicUtil.isValidTopicFilter("$oshare", 10, 4, 100));
        assertTrue(TopicUtil.isValidTopicFilter("$shared/", 10, 4, 10));
        assertTrue(TopicUtil.isValidTopicFilter("$oshared/", 10, 4, 100));
        assertTrue(TopicUtil.isValidTopicFilter("$share/g/", 10, 4, 100));
        assertTrue(TopicUtil.isValidTopicFilter("$share/g//", 10, 4, 100));
        assertFalse(TopicUtil.isValidTopicFilter("$share/g/abcdef/", 5, 4, 10));
        assertFalse(TopicUtil.isValidTopicFilter("$share/g/1/2/3/4/5", 5, 4, 255));
        assertFalse(TopicUtil.isValidTopicFilter("$share/g//1/2/3/4", 5, 4, 255));
        assertFalse(TopicUtil.isValidTopicFilter("$share/g//1/2/3/", 5, 4, 255));

        assertTrue(TopicUtil.isValidTopicFilter("$share/g/+/a", 10, 4, 100));
        assertTrue(TopicUtil.isValidTopicFilter("$share/g/#", 10, 4, 100));
        assertTrue(TopicUtil.isValidTopicFilter("$share/g//#", 10, 4, 100));
        assertTrue(TopicUtil.isValidTopicFilter("$share/g//+/a/#", 10, 4, 100));

        assertFalse(TopicUtil.isValidTopicFilter("$share/g//a+", 10, 4, 100));
        assertFalse(TopicUtil.isValidTopicFilter("$share/g/+a", 10, 4, 100));
        assertFalse(TopicUtil.isValidTopicFilter("$share/g/#/a", 10, 4, 100));
    }

    @Test
    public void normalTopicFilterSerde() {
        String tf = "a/b/c";
        RouteMatcher filter = from(tf);
        assertEquals(filter.getType(), RouteMatcher.Type.Normal);
        assertEquals(filter.getFilterLevelList(), parse(tf, false));
        assertFalse(filter.hasGroup());
        assertEquals(filter.getMqttTopicFilter(), tf);
    }

    @Test
    public void unorderedSharedTopicFilterSerde() {
        String tf = "$share/group/a/b/c";
        RouteMatcher filter = from(tf);
        assertEquals(filter.getType(), RouteMatcher.Type.UnorderedShare);
        assertEquals(filter.getFilterLevelList(), parse("a/b/c", false));
        assertEquals(filter.getGroup(), "group");
        assertEquals(filter.getMqttTopicFilter(), tf);

        tf = "$share/group//a/b/c";
        filter = from(tf);
        assertEquals(filter.getType(), RouteMatcher.Type.UnorderedShare);
        assertEquals(filter.getFilterLevelList(), parse("/a/b/c", false));
        assertEquals(filter.getGroup(), "group");
        assertEquals(filter.getMqttTopicFilter(), tf);
    }

    @Test
    public void orderedSharedTopicFilterSerde() {
        String tf = "$oshare/group/a/b/c";
        RouteMatcher filter = from(tf);
        assertEquals(filter.getType(), RouteMatcher.Type.OrderedShare);
        assertEquals(filter.getFilterLevelList(), parse("a/b/c", false));
        assertEquals(filter.getGroup(), "group");
        assertEquals(filter.getMqttTopicFilter(), tf);

        tf = "$oshare/group//a/b/c";
        filter = from(tf);
        assertEquals(filter.getType(), RouteMatcher.Type.OrderedShare);
        assertEquals(filter.getFilterLevelList(), parse("/a/b/c", false));
        assertEquals(filter.getGroup(), "group");
        assertEquals(filter.getMqttTopicFilter(), tf);
    }
}
