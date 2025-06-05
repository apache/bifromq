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

package org.apache.bifromq.dist;


import static org.apache.bifromq.dist.TestUtil.findNext;
import static org.apache.bifromq.util.TopicConst.MULTI_WILDCARD;
import static org.apache.bifromq.util.TopicConst.SINGLE_WILDCARD;
import static org.apache.bifromq.util.TopicConst.SYS_PREFIX;

import org.apache.bifromq.util.TopicUtil;
import java.util.List;
import java.util.Optional;

public class TopicMatcher {
    private final List<String> topicLevels;

    public TopicMatcher(String topic) {
        topicLevels = TopicUtil.parse(topic, false);
    }

    public boolean match(String escapedTopicFilter) {
        List<String> filterLevels = TopicUtil.parse(escapedTopicFilter, true);
        boolean matched = false;
        int hasMatched = 0;
        out:
        for (int i = 0; i < Math.min(topicLevels.size(), filterLevels.size()); i++) {
            String topicLevel = topicLevels.get(i);
            String filterLevel = filterLevels.get(i);
            switch (filterLevel) {
                case MULTI_WILDCARD:
                    if (i == 0 && topicLevel.startsWith(SYS_PREFIX)) {
                        // system topic not matched by first #
                        break out;
                    }
                    hasMatched++;
                    matched = true;
                    break out;
                case SINGLE_WILDCARD:
                    if (i == 0 && topicLevel.startsWith(SYS_PREFIX)) {
                        // system topic not matched by first +
                        break out;
                    }
                    hasMatched++;
                    // if all level matched
                    if (hasMatched == topicLevels.size()) {
                        // if filter has the same level
                        if (topicLevels.size() == filterLevels.size()) {
                            matched = true;
                            break out;
                        }
                        // if filter has one more level and its "#"
                        if (topicLevels.size() + 1 == filterLevels.size()
                            && filterLevels.get(i + 1).equals(MULTI_WILDCARD)) {
                            matched = true;
                            break out;
                        }
                    }
                    break;
                default:
                    if (topicLevel.equals(filterLevel)) {
                        hasMatched++;
                        // if all level matched
                        if (hasMatched == topicLevels.size()) {
                            // if filter has the same level
                            if (topicLevels.size() == filterLevels.size()) {
                                matched = true;
                                break out;
                            }
                            // if filter has one more level and its "#"
                            if (topicLevels.size() + 1 == filterLevels.size()
                                && filterLevels.get(i + 1).equals(MULTI_WILDCARD)) {
                                matched = true;
                                break out;
                            }
                        }
                    } else {
                        // if mismatch in current level, stop matching
                        break out;
                    }
            }
        }
        return matched;
    }

    public Optional<String> next(String escapedTopicFilter) {
        return findNext(topicLevels, escapedTopicFilter);
    }
}
