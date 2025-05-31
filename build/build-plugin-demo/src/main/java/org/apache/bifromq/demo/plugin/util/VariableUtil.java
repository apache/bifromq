/*
 * Copyright (c) 2025. The BifroMQ Authors. All Rights Reserved.
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

package org.apache.bifromq.demo.plugin.util;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class VariableUtil {
    private static final String PLUGIN_PROMETHEUS_PORT = "PLUGIN_PROMETHEUS_PORT";
    private static final String PLUGIN_PROMETHEUS_CONTEXT = "PLUGIN_PROMETHEUS_CONTEXT";

    private static final int DEFAULT_PORT = 9090;
    private static final String DEFAULT_CONTEXT = "/metrics";

    public static int getPort() {
        String prometheusPort = System.getenv(PLUGIN_PROMETHEUS_PORT);
        if (prometheusPort == null) {
            prometheusPort = System.getProperty(PLUGIN_PROMETHEUS_PORT, "9090");
        }
        try {
            return Integer.parseUnsignedInt(prometheusPort);
        } catch (Throwable e) {
            log.error("Parse prometheus port: {} error, use default port", prometheusPort, e);
            return DEFAULT_PORT;
        }
    }

    public static String getContext() {
        String ctx = System.getenv(PLUGIN_PROMETHEUS_CONTEXT);
        if (ctx == null) {
            ctx = System.getProperty(PLUGIN_PROMETHEUS_CONTEXT, DEFAULT_CONTEXT);
        }
        return ctx.startsWith("/") ? ctx : "/" + ctx;
    }
}
