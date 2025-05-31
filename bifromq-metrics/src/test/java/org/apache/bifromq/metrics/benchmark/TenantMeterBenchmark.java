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

package org.apache.bifromq.metrics.benchmark;

import org.apache.bifromq.metrics.TenantMetric;
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
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

public class TenantMeterBenchmark {
    private static PrometheusMeterRegistry registry;
    private static HttpServer prometheusExportServer;
    private static Thread serverThread;

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @Warmup(iterations = 3)
    @Measurement(iterations = 4)
    @Threads(4)
    @Fork(0)
    public void timer(TenantMeterBenchmarkState state) {
        state.meter.timer(TenantMetric.MqttQoS0InternalLatency)
            .record(ThreadLocalRandom.current().nextLong(0, 10000), TimeUnit.MILLISECONDS);
    }


    @SneakyThrows
    public static void main(String[] args) {
        registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        registry.config().meterFilter(new MeterFilter() {
            @Override
            public DistributionStatisticConfig configure(Meter.Id id, DistributionStatisticConfig config) {
                switch (id.getType()) {
                    case TIMER:
                    case DISTRIBUTION_SUMMARY:
                        return DistributionStatisticConfig.builder()
                            .percentiles(0.5, 0.95, 0.99)
                            .build();
                }
                return config;
            }
        });
        Metrics.addRegistry(registry);
        try {
            prometheusExportServer = HttpServer.create(
                new InetSocketAddress(8888), 0);
            prometheusExportServer.createContext("/", httpExchange -> {
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
        Options opt = new OptionsBuilder()
            .include(TenantMeterBenchmark.class.getSimpleName())
            .build();
        new Runner(opt).run();
    }
}
