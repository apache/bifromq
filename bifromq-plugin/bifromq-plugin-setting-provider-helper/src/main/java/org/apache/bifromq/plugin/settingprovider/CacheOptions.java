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

package org.apache.bifromq.plugin.settingprovider;

import java.time.Duration;
import lombok.extern.slf4j.Slf4j;

public interface CacheOptions {
    CacheOptions DEFAULT = new SettingCacheOptions();

    Duration refreshDuration();

    int maxCachedTenants();

    Duration expireDuration();

    boolean provideInitValue();

    @Slf4j
    class SettingCacheOptions implements CacheOptions {
        public static final String SYS_PROP_PROVIDE_INIT_VALUE = "setting_provide_init_value";
        public static final String SYS_PROP_SETTING_REFRESH_SECONDS = "setting_refresh_seconds";
        public static final String SYS_PROP_SETTING_EXPIRE_SECONDS = "setting_expire_seconds";
        public static final String SYS_PROP_SETTING_TENANT_CACHE_LIMIT = "setting_tenant_cache_limit";
        private static final Duration DEFAULT_SETTING_REFRESH_SECONDS = Duration.ofSeconds(5);
        private static final Duration DEFAULT_SETTING_EXPIRE_SECONDS = Duration.ofSeconds(300);
        private static final int DEFAULT_MAX_CACHED_TENANTS = 100;
        private static final boolean PROVIDE_INIT_VALUE = false;

        @Override
        public boolean provideInitValue() {
            String override = System.getProperty(SYS_PROP_PROVIDE_INIT_VALUE);
            if (override != null) {
                try {
                    return Boolean.parseBoolean(override);
                } catch (Throwable e) {
                    log.error("Unable to parse 'setting_provide_init_value' value from system property: value={}",
                        override);
                }
            }
            return PROVIDE_INIT_VALUE;
        }

        public Duration refreshDuration() {
            String override = System.getProperty(SYS_PROP_SETTING_REFRESH_SECONDS);
            if (override != null) {
                try {
                    return Duration.ofSeconds(Long.parseLong(override));
                } catch (Throwable e) {
                    log.error("Unable to parse 'setting_refresh_seconds' value from system property: value={}",
                        override);
                }
            }
            return DEFAULT_SETTING_REFRESH_SECONDS;
        }

        public int maxCachedTenants() {
            String override = System.getProperty(SYS_PROP_SETTING_TENANT_CACHE_LIMIT);
            if (override != null) {
                try {
                    return Integer.parseInt(override);
                } catch (Throwable e) {
                    log.error("Unable to parse 'setting_tenant_cache_limit' value from system property: value={}",
                        override);
                }
            }
            return DEFAULT_MAX_CACHED_TENANTS;
        }

        public Duration expireDuration() {
            String override = System.getProperty(SYS_PROP_SETTING_EXPIRE_SECONDS);
            if (override != null) {
                try {
                    return Duration.ofSeconds(Long.parseLong(override));
                } catch (Throwable e) {
                    log.error("Unable to parse 'setting_expire_seconds' value from system property: value={}",
                        override);
                }
            }
            return DEFAULT_SETTING_EXPIRE_SECONDS;
        }

    }
}
