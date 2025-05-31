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

package org.apache.bifromq.dist.trie;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Fixtures {
    public static final Map<String, List<String>> GlobalTopicToFilters = new HashMap<>();

    public static final Map<String, List<String>> LocalTopicToFilters = new HashMap<>();

    static {
        GlobalTopicToFilters.put("tenantA/a", List.of(
            "tenantA/#",
            "tenantA/+",
            "tenantA/+/#",
            "tenantA/a",
            "tenantA/a/#"
        ));
        GlobalTopicToFilters.put("tenantA/a/b", List.of(
            "tenantA/#",
            "tenantA/+/#",
            "tenantA/+/+",
            "tenantA/+/+/#",
            "tenantA/+/b",
            "tenantA/+/b/#",
            "tenantA/a/#",
            "tenantA/a/+",
            "tenantA/a/+/#",
            "tenantA/a/b",
            "tenantA/a/b/#"
        ));

        GlobalTopicToFilters.put("tenantA/$sys/a", List.of(
            "tenantA/$sys/#",
            "tenantA/$sys/+",
            "tenantA/$sys/+/#",
            "tenantA/$sys/a",
            "tenantA/$sys/a/#"
        ));

        GlobalTopicToFilters.put("tenantA//", List.of(
            "tenantA//",
            "tenantA///#",
            "tenantA//#",
            "tenantA//+",
            "tenantA//+/#",
            "tenantA/#",
            "tenantA/+/",
            "tenantA/+//#",
            "tenantA/+/#",
            "tenantA/+/+",
            "tenantA/+/+/#"
        ));

        LocalTopicToFilters.put("a", List.of(
            "#",
            "+",
            "+/#",
            "a",
            "a/#"
        ));


        LocalTopicToFilters.put("$sys/a", List.of(
            "$sys/#",
            "$sys/+",
            "$sys/+/#",
            "$sys/a",
            "$sys/a/#"
        ));

        LocalTopicToFilters.put("/", List.of(
            "/",
            "//#",
            "/#",
            "/+",
            "/+/#",
            "#",
            "+/",
            "+//#",
            "+/#",
            "+/+",
            "+/+/#"
        ));
    }
}
