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

package org.apache.bifromq.util;

/**
 * The constant elements in MQTT Topics and TopicFilters.
 */
public class TopicConst {
    public static final char NUL_CHAR = '\u0000';
    public static final char DELIMITER_CHAR = '/';
    public static final char SINGLE_WILDCARD_CHAR = '+';
    public static final char MULTIPLE_WILDCARD_CHAR = '#';
    public static final String NUL = "\u0000";
    public static final String DELIMITER = "/";
    public static final String SYS_PREFIX = "$";
    public static final String SINGLE_WILDCARD = "+";
    public static final String MULTI_WILDCARD = "#";
    public static final String UNORDERED_SHARE = "$share";
    public static final String ORDERED_SHARE = "$oshare";
}
