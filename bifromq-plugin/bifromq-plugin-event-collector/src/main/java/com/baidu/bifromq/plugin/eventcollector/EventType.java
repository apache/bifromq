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

package com.baidu.bifromq.plugin.eventcollector;

public enum EventType {
    // mqttbroker

    // channel close related events
    AUTH_ERROR,
    UNAUTHENTICATED_CLIENT,
    NOT_AUTHORIZED_CLIENT,
    CHANNEL_ERROR,
    CONNECT_TIMEOUT,
    IDENTIFIER_REJECTED, // exceed max length
    MALFORMED_CLIENT_IDENTIFIER, // malformed utf8
    PROTOCOL_ERROR, // malformed utf8
    MALFORMED_USERNAME, // malformed utf8
    MALFORMED_WILL_TOPIC, // malformed utf8
    UNACCEPTED_PROTOCOL_VER,
    // client connected
    CLIENT_CONNECTED,

    // client disconnect related
    BAD_PACKET,
    BY_CLIENT,
    BY_SERVER,
    CLIENT_CHANNEL_ERROR,
    IDLE,
    INBOX_TRANSIENT_ERROR,
    INVALID_TOPIC,
    MALFORMED_TOPIC, // malformed utf8
    INVALID_TOPIC_FILTER,
    MALFORMED_TOPIC_FILTER, // malformed utf8
    KICKED,
    NO_PUB_PERMISSION,
    PROTOCOL_VIOLATION,
    SESSION_CHECK_ERROR,
    SESSION_CLEANUP_ERROR,
    SESSION_CREATE_ERROR,
    TOO_LARGE_SUBSCRIPTION,
    TOO_LARGE_UNSUBSCRIPTION,
    RETAIN_NOT_SUPPORTED,

    PING_REQ,
    DISCARD,
    WILL_DISTED,

    WILL_DIST_ERROR,
    QOS0_DIST_ERROR,
    QOS1_DIST_ERROR,
    QOS2_DIST_ERROR,

    PUB_ACKED,
    PUB_ACK_DROPPED,
    PUB_RECED,
    PUB_REC_DROPPED,
    MSG_RETAINED,
    RETAIN_MSG_CLEARED,
    MSG_RETAINED_ERROR,
    MATCH_RETAIN_ERROR,
    QOS0_PUSHED,
    QOS0_DROPPED,
    QOS1_PUSHED,
    QOS1_DROPPED,
    QOS1_CONFIRMED,
    QOS2_PUSHED,
    QOS2_RECEIVED,
    QOS2_DROPPED,
    QOS2_CONFIRMED,
    PUB_ACTION_DISALLOW,
    SUB_ACTION_DISALLOW,
    UNSUB_ACTION_DISALLOW,
    ACCESS_CONTROL_ERROR,
    SUB_ACKED,
    UNSUB_ACKED,

    // dist service
    DISTED,
    DIST_ERROR,
    DELIVER_ERROR,
    DELIVER_NO_INBOX,
    DELIVERED,
    SUBSCRIBED,
    SUBSCRIBE_ERROR,
    UNSUBSCRIBED,
    UNSUBSCRIBED_ERROR,

    // inbox service
    OVERFLOWED
}
