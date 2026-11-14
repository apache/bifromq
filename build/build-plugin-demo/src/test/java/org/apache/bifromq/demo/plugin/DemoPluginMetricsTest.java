/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.    
 */

package org.apache.bifromq.demo.plugin;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import org.apache.bifromq.plugin.BifroMQPluginDescriptor;
import org.pf4j.DefaultPluginDescriptor;
import org.testng.annotations.Test;

public class DemoPluginMetricsTest {
    @Test
    public void servesPrometheusTextOverHttp() throws Exception {
        String oldPort = System.getProperty("PLUGIN_PROMETHEUS_PORT");
        String oldContext = System.getProperty("PLUGIN_PROMETHEUS_CONTEXT");
        int port;
        try (ServerSocket socket = new ServerSocket(0, 0, InetAddress.getLoopbackAddress())) {
            port = socket.getLocalPort();
        }
        System.setProperty("PLUGIN_PROMETHEUS_PORT", Integer.toString(port));
        System.setProperty("PLUGIN_PROMETHEUS_CONTEXT", "/compat-metrics");
        DemoPlugin plugin = null;
        Counter counter = null;
        try {
            plugin = new DemoPlugin(new BifroMQPluginDescriptor(new DefaultPluginDescriptor(), Path.of("."),
                getClass().getClassLoader(), true));
            plugin.start();
            counter = Counter.builder("compat.requests").tag("tenant", "compat").register(Metrics.globalRegistry);
            counter.increment(2);
            HttpRequest request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/compat-metrics"))
                .timeout(Duration.ofSeconds(5)).GET().build();
            HttpResponse<String> response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
            assertEquals(response.statusCode(), 200);
            assertEquals(response.headers().firstValue("Content-Type").orElseThrow(),
                "text/plain; version=0.0.4; charset=utf-8");
            assertTrue(response.body().contains("# TYPE compat_requests_total counter"));
            double value = response.body().lines().filter(line -> line.startsWith("compat_requests_total{"))
                .mapToDouble(line -> Double.parseDouble(line.substring(line.indexOf('}') + 2)))
                .findFirst().orElseThrow();
            assertEquals(value, 2.0);
        } finally {
            if (plugin != null) {
                plugin.stop();
            }
            if (counter != null) {
                Metrics.globalRegistry.remove(counter);
            }
            restoreProperty("PLUGIN_PROMETHEUS_PORT", oldPort);
            restoreProperty("PLUGIN_PROMETHEUS_CONTEXT", oldContext);
        }
    }

    private static void restoreProperty(String name, String value) {
        if (value == null) {
            System.clearProperty(name);
        } else {
            System.setProperty(name, value);
        }
    }
}
