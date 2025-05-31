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

package org.apache.bifromq.mqtt.inbox.util;

import org.apache.bifromq.sysprops.props.DeliverersPerMqttServer;

public class DelivererKeyUtil {
    private static final String DELIMITER = ":";
    private static final int MAX_DELIVERER_NUM = DeliverersPerMqttServer.INSTANCE.get();

    public static String toDelivererKey(String tenantId, String inboxId, String serverId) {
        assert !serverId.contains(DELIMITER) : "serverId SHOULD NOT contain '" + DELIMITER + "'";
        return serverId + DELIMITER + tenantId + DELIMITER + groupIdx(inboxId);
    }

    public static String parseTenantId(String delivererKey) {
        int firstIdx = delivererKey.indexOf(DELIMITER);
        int secondIdx = delivererKey.lastIndexOf(DELIMITER);
        return delivererKey.substring(firstIdx + 1, secondIdx);
    }

    public static String parseServerId(String delivererKey) {
        return delivererKey.substring(0, delivererKey.indexOf(DELIMITER));
    }

    private static int groupIdx(String inboxId) {
        int idx = inboxId.hashCode() % MAX_DELIVERER_NUM;
        if (idx < 0) {
            idx = (idx + MAX_DELIVERER_NUM) % MAX_DELIVERER_NUM;
        }
        return idx;
    }
}
