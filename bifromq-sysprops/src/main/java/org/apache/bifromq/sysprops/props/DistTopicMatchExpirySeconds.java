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

package org.apache.bifromq.sysprops.props;

import org.apache.bifromq.sysprops.BifroMQSysProp;
import org.apache.bifromq.sysprops.parser.IntegerParser;

/**
 * The system property for the expiry seconds of dist topic match.
 */
public final class DistTopicMatchExpirySeconds extends BifroMQSysProp<Integer, IntegerParser> {
    public static final DistTopicMatchExpirySeconds INSTANCE = new DistTopicMatchExpirySeconds();

    private DistTopicMatchExpirySeconds() {
        super("dist_worker_topic_match_expiry_seconds", 60, IntegerParser.POSITIVE);
    }
}
