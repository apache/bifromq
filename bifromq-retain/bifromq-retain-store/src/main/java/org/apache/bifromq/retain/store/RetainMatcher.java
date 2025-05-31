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

package org.apache.bifromq.retain.store;

import java.util.List;

public class RetainMatcher {
    public enum MatchResult {
        MATCHED_AND_STOP,
        MATCHED_AND_CONTINUE,
        MISMATCH_AND_CONTINUE,
        MISMATCH_AND_STOP
    }

    /**
     * The method is used to match topic against wildcard topic filter during scanning ordered retained records The
     * retained message records are ordered by topic level number and then by topic level string.
     */
    public static MatchResult match(List<String> topicLevels, List<String> matchLevels) {
        boolean matchMultiLevel = matchLevels.get(matchLevels.size() - 1).equals("#");
        if (matchMultiLevel && topicLevels.size() + 1 < matchLevels.size()) {
            // match multi-level wildcard but topic levels are less than match levels
            return MatchResult.MISMATCH_AND_STOP;
        }
        if (!matchMultiLevel && topicLevels.size() != matchLevels.size()) {
            // not match multi-level wildcard but topic levels are not equal to match levels
            return MatchResult.MISMATCH_AND_STOP;
        }
        boolean singleLevelMatched = false;
        for (int i = 0; i < topicLevels.size(); i++) {
            String matchLevel = matchLevels.get(i);
            switch (matchLevel) {
                case "+":
                    singleLevelMatched = true;
                    continue;
                case "#":
                    return MatchResult.MATCHED_AND_CONTINUE;
                default:
                    int c = matchLevel.compareTo(topicLevels.get(i));
                    if (c != 0) {
                        if (singleLevelMatched) {
                            if (c < 0) {
                                return MatchResult.MISMATCH_AND_STOP;
                            }
                            return MatchResult.MISMATCH_AND_CONTINUE;
                        }
                        if (matchMultiLevel) {
                            if (matchLevel.isEmpty()) {
                                // match '/#'
                                return MatchResult.MATCHED_AND_CONTINUE;
                            }
                            return MatchResult.MISMATCH_AND_CONTINUE;
                        }
                        return MatchResult.MISMATCH_AND_STOP;
                    }
            }
        }
        if (singleLevelMatched || matchMultiLevel) {
            return MatchResult.MATCHED_AND_CONTINUE;
        }
        return MatchResult.MATCHED_AND_STOP;
    }
}
