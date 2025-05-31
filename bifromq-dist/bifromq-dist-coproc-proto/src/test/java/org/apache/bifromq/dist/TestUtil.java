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

package org.apache.bifromq.dist;

import static org.apache.bifromq.util.TopicConst.DELIMITER;
import static org.apache.bifromq.util.TopicConst.MULTI_WILDCARD;
import static org.apache.bifromq.util.TopicConst.NUL;
import static org.apache.bifromq.util.TopicConst.SINGLE_WILDCARD;
import static org.apache.bifromq.util.TopicConst.SYS_PREFIX;
import static org.apache.bifromq.util.TopicUtil.fastJoin;
import static org.apache.bifromq.util.TopicUtil.parse;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.newLinkedList;
import static java.util.Collections.singleton;

import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

public class TestUtil {
    public static String randomTopic() {
        return randomTopic(16);
    }

    public static String randomTopicFilter() {
        return randomTopicFilter(16);
    }

    public static String randomTopic(int maxLevel) {
        int levels = ThreadLocalRandom.current().nextInt(1, maxLevel + 1);
        String[] topicLevels = new String[levels];
        for (int i = 0; i < levels; i++) {
            topicLevels[i] = RandomTopicName.nextString(8);
        }
        if (ThreadLocalRandom.current().nextFloat() > 0.5) {
            topicLevels[0] = SYS_PREFIX + topicLevels[0];// make
        }
        String topic = String.join(DELIMITER, topicLevels);
        return ThreadLocalRandom.current().nextFloat() > 0.5 ? DELIMITER + topic : topic;
    }

    public static String randomTopicFilter(int maxLevel) {
        int levels = ThreadLocalRandom.current().nextInt(1, maxLevel + 1);
        String[] filterLevels = new String[levels];
        for (int i = 0; i < levels; i++) {
            filterLevels[i] = RandomTopicName.nextString(8, true, i < levels - 1);
        }
        return String.join(DELIMITER, filterLevels);
    }

    public static List<String> expand(String topic) {
        List<String> topicLevels = parse(topic, false);
        List<String> topicFilters = newArrayList();
        LinkedList<LinkedList<String>> toVisit = newLinkedList();
        String rootLevel = topicLevels.get(0);
        if (rootLevel.startsWith(SYS_PREFIX)) {
            // sys topic don't match "#" and "+"
            toVisit.add(newLinkedList(singleton(rootLevel)));
        } else {
            toVisit.addAll(toFilters(rootLevel));
        }
        while (!toVisit.isEmpty()) {
            LinkedList<String> currentFilterLevels = toVisit.pollFirst();
            switch (currentFilterLevels.getLast()) {
                case MULTI_WILDCARD:
                    topicFilters.add(fastJoin(NUL, currentFilterLevels));
                    break;
                case SINGLE_WILDCARD:
                default:
                    if (currentFilterLevels.size() == topicLevels.size()) {
                        String tf = fastJoin(NUL, currentFilterLevels);
                        topicFilters.add(tf);
                        if (!currentFilterLevels.getLast().isEmpty()) {
                            // "#" also matches parent level if current level name is not empty string
                            topicFilters.add(fastJoin(NUL, List.of(tf, MULTI_WILDCARD)));
                        }
                    } else {
                        toFilters(topicLevels.get(currentFilterLevels.size()))
                            .descendingIterator()
                            .forEachRemaining(f -> {
                                currentFilterLevels.descendingIterator().forEachRemaining(f::addFirst);
                                toVisit.addFirst(f);
                            });
                    }
            }
        }
        return topicFilters;
    }

    public static Optional<String> findNext(List<String> topicLevels, String escapedTopicFilter) {
        // the algorithm is O(log(n))
        String rootLevel = topicLevels.get(0);
        LinkedList<LinkedList<String>> toVisit = newLinkedList();
        if (rootLevel.startsWith(SYS_PREFIX)) {
            // sys topic don't match "#" and "+"
            toVisit.add(newLinkedList(singleton(rootLevel)));
        } else {
            toVisit.addAll(toFilters(rootLevel));
        }

        String nextFilter = null;
        out:
        while (!toVisit.isEmpty()) {
            LinkedList<String> currentFilterLevels = toVisit.pollFirst();
            switch (currentFilterLevels.getLast()) {
                case MULTI_WILDCARD: {
                    String current = fastJoin(NUL, currentFilterLevels);
                    if (escapedTopicFilter.compareTo(current) < 0) {
                        nextFilter = current;
                        break out;
                    } else {
                        while (!toVisit.isEmpty()) {
                            String next = fastJoin(NUL, toVisit.peekFirst());
                            if (escapedTopicFilter.compareTo(next) <= 0 || escapedTopicFilter.startsWith(next)) {
                                break;
                            } else {
                                toVisit.pollFirst();
                            }
                        }
                    }
                    break;
                }
                case SINGLE_WILDCARD:
                default: {
                    if (currentFilterLevels.size() == topicLevels.size()) {
                        String current = fastJoin(NUL, currentFilterLevels);
                        if (escapedTopicFilter.compareTo(current) < 0) {
                            nextFilter = current;
                            break out;
                        }
                        current = fastJoin(NUL, List.of(current, MULTI_WILDCARD));
                        if (escapedTopicFilter.compareTo(current) < 0) {
                            nextFilter = current;
                            break out;
                        }
                        while (!toVisit.isEmpty()) {
                            String next = fastJoin(NUL, toVisit.peekFirst());
                            if (escapedTopicFilter.compareTo(next) <= 0 || escapedTopicFilter.startsWith(next)) {
                                break;
                            } else {
                                toVisit.pollFirst();
                            }
                        }
                    } else {
                        toFilters(topicLevels.get(currentFilterLevels.size())).descendingIterator()
                            .forEachRemaining(f -> {
                                currentFilterLevels.descendingIterator().forEachRemaining(f::addFirst);
                                toVisit.addFirst(f);
                            });
                    }

                }
            }
        }
        return Optional.ofNullable(nextFilter);
    }

    private static LinkedList<LinkedList<String>> toFilters(String topicLevel) {
        LinkedList<LinkedList<String>> filters = newLinkedList();
        if (MULTI_WILDCARD.compareTo(topicLevel) > 0) {
            filters.add(newLinkedList(singleton(topicLevel)));
            filters.add(newLinkedList(singleton(MULTI_WILDCARD)));
            filters.add(newLinkedList(singleton(SINGLE_WILDCARD)));
        } else if (SINGLE_WILDCARD.compareTo(topicLevel) > 0) {
            filters.add(newLinkedList(singleton(MULTI_WILDCARD)));
            filters.add(newLinkedList(singleton(topicLevel)));
            filters.add(newLinkedList(singleton(SINGLE_WILDCARD)));
        } else {
            filters.add(newLinkedList(singleton(MULTI_WILDCARD)));
            filters.add(newLinkedList(singleton(SINGLE_WILDCARD)));
            filters.add(newLinkedList(singleton(topicLevel)));
        }
        return filters;
    }

    private static class RandomTopicName {
        public static final String upper = "ABCDEFGHIJKLMNOPQRSTUVWXYZ你好😄";

        public static final String lower = upper.toLowerCase(Locale.ROOT);

        public static final String digits = "0123456789";

        public static final String puncs = " !\"$%&'()*,-.";

        private static final ThreadLocalRandom random = ThreadLocalRandom.current();

        private static final char[] symbols = (upper + lower + digits + puncs).toCharArray();

        public static String nextString(int maxLength) {
            int strLen = random.nextInt(1, maxLength);
            char[] buf = new char[strLen];
            for (int idx = 0; idx < buf.length; ++idx) {
                buf[idx] = symbols[random.nextInt(symbols.length)];
            }
            return new String(buf);
        }

        public static String nextString(int maxLength, boolean includeWildcard, boolean singleWildcardOnly) {
            if (includeWildcard && random.nextFloat() > 0.5) {
                if (singleWildcardOnly) {
                    return "+";
                } else {
                    return "#";
                }
            } else {
                return nextString(maxLength);
            }
        }
    }
}
