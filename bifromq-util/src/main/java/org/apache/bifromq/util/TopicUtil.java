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

package org.apache.bifromq.util;

import static org.apache.bifromq.util.TopicConst.DELIMITER;
import static org.apache.bifromq.util.TopicConst.DELIMITER_CHAR;
import static org.apache.bifromq.util.TopicConst.MULTIPLE_WILDCARD_CHAR;
import static org.apache.bifromq.util.TopicConst.MULTI_WILDCARD;
import static org.apache.bifromq.util.TopicConst.NUL;
import static org.apache.bifromq.util.TopicConst.NUL_CHAR;
import static org.apache.bifromq.util.TopicConst.ORDERED_SHARE;
import static org.apache.bifromq.util.TopicConst.SINGLE_WILDCARD_CHAR;
import static org.apache.bifromq.util.TopicConst.UNORDERED_SHARE;

import org.apache.bifromq.type.RouteMatcher;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

public class TopicUtil {
    private static final String PREFIX_UNORDERED_SHARE = UNORDERED_SHARE + DELIMITER_CHAR;
    private static final String PREFIX_ORDERED_SHARE = ORDERED_SHARE + DELIMITER_CHAR;

    public static boolean isValidTopic(String topic, int maxLevelLength, int maxLevel, int maxLength) {
        assert maxLength <= 65535 && maxLevelLength <= maxLength;
        if (topic.isEmpty() || topic.length() > maxLength) {
            // [MQTT-4.7.3-1]
            return false;
        }
        if (topic.startsWith(PREFIX_ORDERED_SHARE) || topic.startsWith(PREFIX_UNORDERED_SHARE)) {
            return false;
        }
        int topicLevelLength = 0;
        int level = 1;
        for (int i = 0; i < topic.length(); i++) {
            if (topic.charAt(i) == DELIMITER_CHAR) {
                if (++level > maxLevel) {
                    return false;
                }
                if (topicLevelLength > maxLevelLength) {
                    return false;
                }
                topicLevelLength = 0;
            } else {
                char c = topic.charAt(i);
                if (c == NUL_CHAR || c == SINGLE_WILDCARD_CHAR || c == MULTIPLE_WILDCARD_CHAR) {
                    // [MQTT-4.7.3-2] and [MQTT-4.7.1-1]
                    return false;
                }
                topicLevelLength++;
            }
        }
        return topicLevelLength <= maxLevelLength;
    }

    public static boolean isValidTopicFilter(String topicFilter, int maxLevelLength, int maxLevel, int maxLength) {
        // TODO: could be optimized further by building a FSM
        if (topicFilter.startsWith(PREFIX_UNORDERED_SHARE)) {
            maxLength += PREFIX_UNORDERED_SHARE.length();
        }
        if (topicFilter.startsWith(PREFIX_ORDERED_SHARE)) {
            maxLength += PREFIX_ORDERED_SHARE.length();
        }
        assert maxLength <= 65535 && maxLevelLength <= maxLength;
        if (topicFilter.isEmpty() || topicFilter.length() > maxLength) {
            // [MQTT-4.7.3-1]
            return false;
        }
        int i = 0;
        int topicLevelLength = 0;
        if (topicFilter.startsWith(PREFIX_ORDERED_SHARE) || topicFilter.startsWith(PREFIX_UNORDERED_SHARE)) {
            // validate share name
            for (i = topicFilter.indexOf(DELIMITER_CHAR) + 1; i < topicFilter.length(); i++) {
                char c = topicFilter.charAt(i);
                if (c == DELIMITER_CHAR) {
                    break;
                }
                if (c == MULTIPLE_WILDCARD_CHAR || c == SINGLE_WILDCARD_CHAR || c == NUL_CHAR) {
                    // [MQTT-4.8.2-2]
                    return false;
                }
                topicLevelLength++;
            }
            if (topicLevelLength == 0) {
                // [MQTT-4.8.2-1]
                return false;
            }
            if (i == topicFilter.length()) {
                // [MQTT-4.8.2-2]
                return false;
            }
            topicLevelLength = 0;
            // skip one separator to real topicFilter start pos
            i++;
        }
        int startIdx = i;
        int level = 1;
        for (; i < topicFilter.length(); i++) {
            if (topicFilter.charAt(i) == DELIMITER_CHAR) {
                if (++level > maxLevel) {
                    return false;
                }
                if (topicLevelLength > maxLevelLength) {
                    return false;
                }
                topicLevelLength = 0;
            } else {
                char c = topicFilter.charAt(i);
                if (c == NUL_CHAR) {
                    // [MQTT-4.7.3-2]
                    return false;
                }
                if (c == MULTIPLE_WILDCARD_CHAR) {
                    if (i != topicFilter.length() - 1) {
                        return false;
                    }
                    if (i != startIdx && topicFilter.charAt(i - 1) != DELIMITER_CHAR) {
                        return false;
                    }
                }
                if (c == SINGLE_WILDCARD_CHAR) {
                    if (i == startIdx) {
                        if (i != topicFilter.length() - 1 && topicFilter.charAt(i + 1) != DELIMITER_CHAR) {
                            return false;
                        }
                    } else if (i == topicFilter.length() - 1) {
                        if (topicFilter.charAt(i - 1) != DELIMITER_CHAR) {
                            return false;
                        }
                    } else {
                        if (topicFilter.charAt(i - 1) != DELIMITER_CHAR
                            || topicFilter.charAt(i + 1) != DELIMITER_CHAR) {
                            return false;
                        }
                    }

                }
                topicLevelLength++;
            }
        }
        if (level > maxLevel) {
            return false;
        }
        return topicLevelLength <= maxLevelLength;
    }

    public static boolean isWildcardTopicFilter(String topicFilter) {
        return topicFilter.indexOf(SINGLE_WILDCARD_CHAR) >= 0 || isMultiWildcardTopicFilter(topicFilter);
    }

    public static boolean isMultiWildcardTopicFilter(String topicFilter) {
        return topicFilter.endsWith(MULTI_WILDCARD);
    }

    public static boolean isSharedSubscription(String topicFilter) {
        return isOrderedShared(topicFilter) || isUnorderedShared(topicFilter);
    }

    public static boolean isNormalTopicFilter(String topicFilter) {
        return !isSharedSubscription(topicFilter);
    }

    public static boolean isUnorderedShared(String topicFilter) {
        return topicFilter.startsWith(PREFIX_UNORDERED_SHARE);
    }

    public static boolean isOrderedShared(String topicFilter) {
        return topicFilter.startsWith(PREFIX_ORDERED_SHARE);
    }

    public static String escape(String topicFilter) {
        assert !topicFilter.contains(NUL);
        return topicFilter.replace(DELIMITER, NUL);
    }

    public static String unescape(String topicFilter) {
        return topicFilter.replace(NUL, DELIMITER);
    }

    public static List<String> parse(String tenantId, String topic, boolean isEscaped) {
        List<String> topicLevels = new ArrayList<>();
        topicLevels.add(tenantId);
        return parse(topic, isEscaped, topicLevels);
    }

    // parse a topic or topic filter string into a list of topic levels
    // eg. "/" -> ["",""], "/a" -> ["",a], "a/" -> [a,""]
    public static List<String> parse(String topic, boolean isEscaped) {
        return parse(topic, isEscaped, new ArrayList<>());
    }

    // parse a topic or topic filter string into a list of topic levels
    // eg. "/" -> ["",""], "/a" -> ["",a], "a/" -> [a,""]
    private static List<String> parse(String topic, boolean isEscaped, List<String> topicLevels) {
        char splitter = isEscaped ? NUL_CHAR : DELIMITER_CHAR;
        StringBuilder tl = new StringBuilder();
        for (int i = 0; i < topic.length(); i++) {
            if (topic.charAt(i) == splitter) {
                topicLevels.add(tl.toString());
                tl.delete(0, tl.length());
            } else {
                tl.append(topic.charAt(i));
            }
        }
        topicLevels.add(tl.toString());
        return topicLevels;
    }

    public static String fastJoin(CharSequence delimiter, Iterable<? extends CharSequence> strings) {
        StringBuilder sb = new StringBuilder();
        Iterator<? extends CharSequence> itr = strings.iterator();
        while (itr.hasNext()) {
            sb.append(itr.next());
            if (itr.hasNext()) {
                sb.append(delimiter);
            }
        }
        return sb.toString();
    }

    public static <T> String fastJoin(CharSequence delimiter, Iterable<T> items,
                                      Function<T, ? extends CharSequence> toCharSequence) {
        StringBuilder sb = new StringBuilder();
        Iterator<T> itr = items.iterator();
        while (itr.hasNext()) {
            sb.append(toCharSequence.apply(itr.next()));
            if (itr.hasNext()) {
                sb.append(delimiter);
            }
        }
        return sb.toString();
    }

    public static RouteMatcher from(String topicFilter) {
        if (isNormalTopicFilter(topicFilter)) {
            return RouteMatcher.newBuilder()
                .setType(RouteMatcher.Type.Normal)
                .addAllFilterLevel(TopicUtil.parse(topicFilter, false))
                .setMqttTopicFilter(topicFilter)
                .build();
        } else {
            String sharePrefix = topicFilter.startsWith(UNORDERED_SHARE) ? UNORDERED_SHARE : ORDERED_SHARE;
            boolean ordered = !topicFilter.startsWith(UNORDERED_SHARE);
            String rest = topicFilter.substring((sharePrefix + DELIMITER_CHAR).length());
            int firstTopicSeparatorIndex = rest.indexOf(DELIMITER_CHAR);
            String shareGroup = rest.substring(0, firstTopicSeparatorIndex);
            return RouteMatcher.newBuilder()
                .setType(ordered ? RouteMatcher.Type.OrderedShare : RouteMatcher.Type.UnorderedShare)
                .addAllFilterLevel(TopicUtil.parse(rest.substring(firstTopicSeparatorIndex + 1), false))
                .setGroup(shareGroup)
                .setMqttTopicFilter(topicFilter)
                .build();
        }
    }
}
