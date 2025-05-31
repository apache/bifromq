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

package org.apache.bifromq.plugin.settingprovider;

import java.util.function.BiPredicate;
import java.util.function.Predicate;
import lombok.extern.slf4j.Slf4j;

/**
 * The tenant-level settings of BifroMQ.
 */
@Slf4j
public enum Setting {
    MQTT3Enabled(Boolean.class, val -> true, true), MQTT4Enabled(Boolean.class, val -> true, true),
    MQTT5Enabled(Boolean.class, val -> true, true), DebugModeEnabled(Boolean.class, val -> true, false),
    ForceTransient(Boolean.class, val -> true, false), ByPassPermCheckError(Boolean.class, val -> true, true),
    PayloadFormatValidationEnabled(Boolean.class, val -> true, true), RetainEnabled(Boolean.class, val -> true, true),
    WildcardSubscriptionEnabled(Boolean.class, val -> true, true),
    SubscriptionIdentifierEnabled(Boolean.class, val -> true, true),
    SharedSubscriptionEnabled(Boolean.class, val -> true, true),
    MaximumQoS(Integer.class, val -> (int) val == 0 || (int) val == 1 || (int) val == 2, 2),
    MaxTopicLevelLength(Integer.class, val -> (int) val > 0, 40),
    MaxTopicLevels(Integer.class, val -> (int) val > 0, 16),
    MaxTopicLength(Integer.class, val -> (int) val > 0 && (int) val < 65536, 255),
    MaxTopicAlias(Integer.class, val -> (int) val >= 0 && (int) val < 65536, 10),
    MaxSharedGroupMembers(Integer.class, val -> (int) val > 0, 200),
    MaxTopicFiltersPerInbox(Integer.class, val -> (int) val > 0, 100),
    MsgPubPerSec(Integer.class, val -> (int) val > 0 && (int) val <= 1000, 200),
    ReceivingMaximum(Integer.class, val -> (int) val > 0 && (int) val <= 65535, 200),
    InBoundBandWidth(Long.class, val -> (long) val >= 0, 512 * 1024L),
    OutBoundBandWidth(Long.class, val -> (long) val >= 0, 512 * 1024L),
    MaxLastWillBytes(Integer.class, val -> (int) val > 0 && (int) val <= 250 * 1024 * 1024, 128),
    MaxUserPayloadBytes(Integer.class, val -> (int) val > 0 && (int) val <= 256 * 1024 * 1024, 256 * 1024),
    MaxResendTimes(Integer.class, val -> (int) val >= 0, 3),
    ResendTimeoutSeconds(Integer.class, val -> (int) val > 0, 10),
    MaxTopicFiltersPerSub(Integer.class, val -> (int) val > 0 && (int) val <= 100, 10),
    MaxSessionExpirySeconds(Integer.class, val -> (int) val > 0 && Integer.compareUnsigned((int) val, 0xFFFFFFFF) <= 0,
        24 * 60 * 60),
    MinSessionExpirySeconds(Integer.class, (val, tenantId) -> {
        int maxSEI;
        if (tenantId == null) {
            maxSEI = Setting.MaxSessionExpirySeconds.initialValue();
        } else {
            maxSEI = Setting.MaxSessionExpirySeconds.current(tenantId);
        }
        return (int) val > 0 && Integer.compareUnsigned((int) val, maxSEI) <= 0;
    }, 60),
    MinKeepAliveSeconds(Integer.class, val -> (int) val > 0 && (int) val < 65535, 60),
    SessionInboxSize(Integer.class, val -> (int) val > 0 && (int) val <= 65535, 1000),
    QoS0DropOldest(Boolean.class, val -> true, false),
    RetainMessageMatchLimit(Integer.class, val -> (int) val >= 0, 10);

    public final Class<?> valueType;
    private final BiPredicate<Object, String> validator;
    private final Object defVal;
    private volatile ISettingProvider settingProvider;

    Setting(Class<?> valueType, Predicate<Object> validator, Object defValue) {
        this(valueType, (val, tenantId) -> validator.test(val), defValue);
    }

    Setting(Class<?> valueType, BiPredicate<Object, String> validator, Object defValue) {
        this.valueType = valueType;
        this.validator = validator;
        this.defVal = resolve(defValue);
        assert isValid(defVal, null);
    }

    /**
     * The current effective setting's value for given tenant.
     *
     * @param tenantId the id of the calling tenant
     * @return The effective value of the setting for the client
     */
    public <R> R current(String tenantId) {
        return settingProvider == null ? initialValue() : settingProvider.provide(this, tenantId);
    }

    /**
     * Validate if provided value is a valid for the setting.
     *
     * @param val the setting value to be verified
     * @param tenantId the id of the calling tenant
     * @return true if the value is valid
     */
    public <R> boolean isValid(R val, String tenantId) {
        if (!valueType.isInstance(val)) {
            return false;
        }
        return this.validator.test(val, tenantId);
    }

    @SuppressWarnings("unchecked")
    <R> R initialValue() {
        return (R) defVal;
    }

    void setProvider(ISettingProvider provider) {
        this.settingProvider = provider;
    }

    Object resolve(Object initial) {
        String override = System.getProperty(name());
        if (override != null) {
            try {
                if (valueType == Integer.class) {
                    return Integer.parseInt(override);
                }
                if (valueType == Long.class) {
                    return Long.parseLong(override);
                }
                if (valueType == Boolean.class) {
                    return Boolean.parseBoolean(override);
                }
            } catch (Throwable e) {
                log.error("Unable to parse setting value from system property: setting={}, value={}", name(), override);
                return initial;
            }
        }
        return initial;
    }
}