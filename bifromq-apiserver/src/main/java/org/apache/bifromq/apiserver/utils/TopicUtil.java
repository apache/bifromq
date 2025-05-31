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

package org.apache.bifromq.apiserver.utils;

import static org.apache.bifromq.plugin.settingprovider.Setting.MaxTopicLength;
import static org.apache.bifromq.plugin.settingprovider.Setting.MaxTopicLevelLength;
import static org.apache.bifromq.plugin.settingprovider.Setting.MaxTopicLevels;
import static org.apache.bifromq.util.TopicUtil.isValidTopicFilter;

import org.apache.bifromq.plugin.settingprovider.ISettingProvider;
import org.apache.bifromq.sysprops.props.SanityCheckMqttUtf8String;
import org.apache.bifromq.util.UTF8Util;

public class TopicUtil {
    public static boolean checkTopicFilter(String topic, String tenantId, ISettingProvider settingProvider) {
        int maxTopicLevelLength = settingProvider.provide(MaxTopicLevelLength, tenantId);
        int maxTopicLevels = settingProvider.provide(MaxTopicLevels, tenantId);
        int maxTopicLength = settingProvider.provide(MaxTopicLength, tenantId);
        return UTF8Util.isWellFormed(topic, SanityCheckMqttUtf8String.INSTANCE.get())
            && isValidTopicFilter(topic, maxTopicLevelLength, maxTopicLevels, maxTopicLength);
    }
}
