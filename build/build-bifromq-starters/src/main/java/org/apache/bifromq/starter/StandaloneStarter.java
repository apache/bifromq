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

package org.apache.bifromq.starter;

import static org.apache.bifromq.starter.utils.ClusterDomainUtil.resolve;
import static org.apache.bifromq.starter.utils.ConfigFileUtil.build;
import static org.apache.bifromq.starter.utils.ConfigFileUtil.serialize;

import org.apache.bifromq.basecluster.IAgentHost;
import org.apache.bifromq.baseenv.EnvProvider;
import org.apache.bifromq.baseenv.MemUsage;
import org.apache.bifromq.plugin.settingprovider.Setting;
import org.apache.bifromq.starter.config.StandaloneConfig;
import org.apache.bifromq.starter.config.StandaloneConfigConsolidator;
import org.apache.bifromq.starter.metrics.netty.PooledByteBufAllocator;
import org.apache.bifromq.starter.module.APIServerModule;
import org.apache.bifromq.starter.module.ConfigModule;
import org.apache.bifromq.starter.module.CoreServiceModule;
import org.apache.bifromq.starter.module.DistServiceModule;
import org.apache.bifromq.starter.module.ExecutorsModule;
import org.apache.bifromq.starter.module.InboxServiceModule;
import org.apache.bifromq.starter.module.MQTTServiceModule;
import org.apache.bifromq.starter.module.PluginModule;
import org.apache.bifromq.starter.module.RPCClientSSLContextModule;
import org.apache.bifromq.starter.module.RPCServerBuilderModule;
import org.apache.bifromq.starter.module.RetainServiceModule;
import org.apache.bifromq.starter.module.ServiceInjectorModule;
import org.apache.bifromq.starter.module.SessionDictServiceModule;
import org.apache.bifromq.starter.module.SharedResourcesHolder;
import org.apache.bifromq.sysprops.BifroMQSysProp;
import org.apache.bifromq.sysprops.props.ClusterDomainResolveTimeoutSeconds;
import com.google.common.base.Strings;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmCompilationMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmHeapPressureMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmInfoMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.netty4.NettyAllocatorMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.binder.system.UptimeMetrics;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.reactivex.rxjava3.plugins.RxJavaPlugins;
import java.io.File;
import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.reflections.Reflections;

@Slf4j
public class StandaloneStarter {
    private static final Options CLI_OPTIONS = new Options()
        .addOption(Option.builder()
            .option("c")
            .longOpt("conf")
            .desc("the conf file for Starter")
            .hasArg(true)
            .optionalArg(false)
            .argName("CONF_FILE")
            .required(true)
            .build());

    static {
        // log unhandled error from rxjava
        RxJavaPlugins.setErrorHandler(e -> log.error("Uncaught RxJava exception", e));
        // log uncaught exception
        Thread.setDefaultUncaughtExceptionHandler(
            (t, e) -> log.error("Caught an uncaught exception in thread[{}]", t.getName(), e));

    }

    private final List<AutoCloseable> closeables = new LinkedList<>();
    private final StandaloneConfig config;
    private final IAgentHost agentHost;
    private final ServiceBootstrapper.BootstrappedServices bootstrappedServices;
    private final SharedResourcesHolder sharedResourcesHolder;

    @Builder
    private StandaloneStarter(StandaloneConfig config,
                              IAgentHost agentHost,
                              ServiceBootstrapper.BootstrappedServices bootstrappedServices,
                              SharedResourcesHolder sharedResourcesHolder) {
        this.config = config;
        this.agentHost = agentHost;
        this.bootstrappedServices = bootstrappedServices;
        this.sharedResourcesHolder = sharedResourcesHolder;
    }

    void start() {
        startSystemMetrics();
        join();
        bootstrappedServices.start();
        log.info("Standalone broker started");
    }

    void stop() {
        bootstrappedServices.stop();
        sharedResourcesHolder.close();
        closeables.forEach(closable -> {
            try {
                closable.close();
            } catch (Exception e) {
                // Never happen
            }
        });
        log.info("Standalone broker stopped");
    }

    private static void printConfigs(StandaloneConfig config) {
        log.info("Available Processors: {}", EnvProvider.INSTANCE.availableProcessors());
        List<String> arguments = ManagementFactory.getRuntimeMXBean().getInputArguments();
        log.info("JVM arguments: \n  {}", String.join("\n  ", arguments));

        log.info("Settings, which can be modified at runtime, allowing for dynamic adjustment of BifroMQ's "
            + "service behavior per tenant. See https://bifromq.io/docs/plugin/setting_provider/");
        log.info("The initial value of each setting could be overridden by JVM arguments like: '-DMQTT5Enabled=false'");
        for (Setting setting : Setting.values()) {
            log.info("Setting: {}={}", setting.name(), setting.current(""));
        }

        log.info("BifroMQ system properties: ");
        Reflections reflections = new Reflections(BifroMQSysProp.class.getPackageName());
        for (Class<? extends BifroMQSysProp> subclass : reflections.getSubTypesOf(BifroMQSysProp.class)) {
            try {
                BifroMQSysProp<?, ?> prop = (BifroMQSysProp<?, ?>) subclass.getField("INSTANCE").get(null);
                log.info("BifroMQSysProp: {}={}", prop.propKey(), prop.get());
            } catch (NoSuchFieldException | IllegalAccessException e) {
                throw new RuntimeException("Failed to access INSTANCE field of subclass: " + subclass.getName(), e);
            }
        }
        log.info("Consolidated Config(YAML): \n{}", serialize(config));
    }

    private void join() {
        String env = config.getClusterConfig().getEnv();
        String clusterDomainName = config.getClusterConfig().getClusterDomainName();
        String seeds = config.getClusterConfig().getSeedEndpoints();
        if (!Strings.isNullOrEmpty(clusterDomainName)) {
            log.debug("AgentHost[{}] join clusterDomainName: {}", env, clusterDomainName);
            resolve(clusterDomainName, Duration.ofSeconds(ClusterDomainResolveTimeoutSeconds.INSTANCE.get()))
                .thenApply(seedAddrs ->
                    Arrays.stream(seedAddrs)
                        .map(addr -> new InetSocketAddress(addr, config.getClusterConfig().getPort()))
                        .collect(Collectors.toSet()))
                .whenComplete((seedEndpoints, e) -> {
                    if (e != null) {
                        log.warn("ClusterDomainName[{}] is unresolvable, due to {}", clusterDomainName, e.getMessage());
                    } else {
                        log.info("ClusterDomainName[{}] resolved to seedEndpoints: {}",
                            clusterDomainName, seedEndpoints);
                        joinSeeds(seedEndpoints);
                    }
                });
        }
        if (!Strings.isNullOrEmpty(seeds)) {
            log.debug("AgentHost[{}] join seedEndpoints: {}", env, seeds);
            Set<InetSocketAddress> seedEndpoints = Arrays.stream(seeds.split(","))
                .map(endpoint -> {
                    String[] hostPort = endpoint.trim().split(":");
                    return new InetSocketAddress(hostPort[0], Integer.parseInt(hostPort[1]));
                }).collect(Collectors.toSet());
            joinSeeds(seedEndpoints);
        }
    }

    private void joinSeeds(Set<InetSocketAddress> seeds) {
        agentHost.join(seeds)
            .whenComplete((v, e) -> {
                if (e != null) {
                    log.warn("AgentHost failed to join seedEndpoint: {}", seeds, e);
                } else {
                    log.info("AgentHost joined seedEndpoint: {}", seeds);
                }
            });
    }

    private void startSystemMetrics() {
        // os metrics
        // disable file descriptor metrics since its too heavy
        new UptimeMetrics().bindTo(Metrics.globalRegistry);
        new ProcessorMetrics().bindTo(Metrics.globalRegistry);
        // jvm metrics
        new JvmInfoMetrics().bindTo(Metrics.globalRegistry);
        new ClassLoaderMetrics().bindTo(Metrics.globalRegistry);
        new JvmCompilationMetrics().bindTo(Metrics.globalRegistry);
        new JvmMemoryMetrics().bindTo(Metrics.globalRegistry);
        new JvmThreadMetrics().bindTo(Metrics.globalRegistry);
        JvmGcMetrics jvmGcMetrics = new JvmGcMetrics();
        jvmGcMetrics.bindTo(Metrics.globalRegistry);
        closeables.add(jvmGcMetrics);
        JvmHeapPressureMetrics jvmHeapPressureMetrics = new JvmHeapPressureMetrics();
        jvmHeapPressureMetrics.bindTo(Metrics.globalRegistry);
        closeables.add(jvmHeapPressureMetrics);
        // using nonblocking version of netty allocator metrics
        new NettyAllocatorMetrics(PooledByteBufAllocator.INSTANCE).bindTo(Metrics.globalRegistry);
        // netty default allocator metrics
        new NettyAllocatorMetrics(UnpooledByteBufAllocator.DEFAULT).bindTo(Metrics.globalRegistry);

        Gauge.builder("netty.direct.memory.usage", () -> MemUsage.local().nettyDirectMemoryUsage())
            .register(Metrics.globalRegistry);
    }

    public static void main(String[] args) {
        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        try {
            CommandLine cmd = parser.parse(CLI_OPTIONS, args);
            File confFile = new File(cmd.getOptionValue("c"));
            if (!confFile.exists()) {
                throw new RuntimeException("Conf file does not exist: " + cmd.getOptionValue("c"));
            }
            StandaloneConfig config = build(confFile, StandaloneConfig.class);
            StandaloneConfigConsolidator.consolidate(config);
            printConfigs(config);

            if (!Strings.isNullOrEmpty(config.getClusterConfig().getEnv())) {
                Metrics.globalRegistry.config().commonTags("env", config.getClusterConfig().getEnv());
            }
            Injector serviceInjector = Guice.createInjector(
                new ConfigModule(config),
                new RPCClientSSLContextModule(),
                new CoreServiceModule(),
                new RPCServerBuilderModule(),
                new PluginModule(),
                new ExecutorsModule());
            Injector injector = Guice.createInjector(
                new ConfigModule(config),
                new DistServiceModule(),
                new InboxServiceModule(),
                new RetainServiceModule(),
                new SessionDictServiceModule(),
                new MQTTServiceModule(),
                new APIServerModule(),
                new ServiceInjectorModule(serviceInjector));
            StandaloneStarter starter = StandaloneStarter.builder()
                .config(config)
                .agentHost(serviceInjector.getInstance(IAgentHost.class))
                .bootstrappedServices(injector.getInstance(ServiceBootstrapper.class).bootstrap())
                .sharedResourcesHolder(serviceInjector.getInstance(SharedResourcesHolder.class))
                .build();
            Thread shutdownThread = new Thread(starter::stop);
            shutdownThread.setName("shutdown");
            Runtime.getRuntime().addShutdownHook(shutdownThread);

            starter.start();
        } catch (Throwable e) {
            log.error("Failed to start BifroMQ", e);
            formatter.printHelp("CMD", CLI_OPTIONS);
            System.exit(-1);
        }
    }
}
