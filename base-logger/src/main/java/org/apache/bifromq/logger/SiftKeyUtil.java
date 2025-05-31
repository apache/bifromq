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

package org.apache.bifromq.logger;

public class SiftKeyUtil {
    /**
     * Build sift key from tags.
     *
     * @param tags tags
     * @return sift key
     */
    public static String buildSiftKey(String... tags) {
        StringBuilder logKey = new StringBuilder();
        for (int i = 0; i < tags.length; i += 2) {
            logKey.append(tags[i + 1]);
            if (i + 2 < tags.length) {
                logKey.append("-");
            }
        }
        return logKey.toString();
    }
}
