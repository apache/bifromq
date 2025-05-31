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

package org.apache.bifromq.demo.plugin;

import static org.apache.bifromq.demo.plugin.util.VariableUtil.getContext;
import static org.apache.bifromq.demo.plugin.util.VariableUtil.getPort;

import com.sun.net.httpserver.HttpServer;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.plugin.BifroMQPlugin;
import org.apache.bifromq.plugin.BifroMQPluginDescriptor;

@Slf4j
public class DemoPlugin extends BifroMQPlugin<DemoPluginContext> {
    private final PrometheusMeterRegistry registry;
    private final HttpServer prometheusExportServer;
    private final Thread serverThread;
    private final int exportPort;

    /**
     * Constructor to be used by plugin manager for plugin instantiation. Your plugins have to provide constructor with
     * this exact signature to be successfully loaded by manager.
     *
     * @param context the context object
     */
    public DemoPlugin(BifroMQPluginDescriptor context) {
        super(context);
        exportPort = getPort();
        registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        registry.config().meterFilter(new MeterFilter() {
            @Override
            public DistributionStatisticConfig configure(Meter.Id id, DistributionStatisticConfig config) {
                switch (id.getType()) {
                    case TIMER:
                    case DISTRIBUTION_SUMMARY:
                        // following config will cause huge performance penalty
                        // don't enable it unless you accept the consequence
//                        if (id.getTag("tenantId") != null) {
//                            return DistributionStatisticConfig.builder()
//                                .percentiles(0.5, 0.99, 0.999)
//                                .expiry(Duration.ofSeconds(5))
//                                .build()
//                                .merge(config);
//                        }
                }
                return DistributionStatisticConfig.builder()
                    .expiry(Duration.ofSeconds(5))
                    .build().merge(config);
            }
        });
        Metrics.addRegistry(registry);
        try {
            prometheusExportServer = HttpServer.create(new InetSocketAddress(exportPort), 0);
            prometheusExportServer.createContext(getContext(), httpExchange -> {
                String response = registry.scrape();
                httpExchange.sendResponseHeaders(200, response.getBytes().length);
                try (OutputStream os = httpExchange.getResponseBody()) {
                    os.write(response.getBytes());
                }
            });
            serverThread = new Thread(prometheusExportServer::start);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void doStart() {
        serverThread.start();
        log.info("Prometheus exporter started on port {}", exportPort);
    }

    @Override
    protected void doStop() {
        prometheusExportServer.stop(0);
        Metrics.removeRegistry(registry);
        log.info("Prometheus exporter stopped");
    }
}
