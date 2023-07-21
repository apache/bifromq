/*
 * Copyright (c) 2023. Baidu, Inc. All Rights Reserved.
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

package com.baidu.bifromq.starter;

import com.baidu.bifromq.basecluster.AgentHostOptions;
import com.baidu.bifromq.basecluster.IAgentHost;
import com.baidu.bifromq.starter.config.StarterConfig;
import com.baidu.bifromq.starter.config.model.AgentHostConfig;
import com.baidu.bifromq.starter.config.model.ClientSSLContextConfig;
import com.baidu.bifromq.starter.config.model.ServerSSLContextConfig;
import com.baidu.bifromq.starter.utils.ResourceUtil;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.base.Strings;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmHeapPressureMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmInfoMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.binder.system.UptimeMetrics;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import io.reactivex.rxjava3.plugins.RxJavaPlugins;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.Provider;
import java.security.Security;
import java.security.cert.CertificateException;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class BaseStarter<T extends StarterConfig> implements IStarter {

    static {
        RxJavaPlugins.setErrorHandler(e -> log.error("Uncaught RxJava exception", e));
    }

    public static final String CONF_DIR_PROP = "CONF_DIR";

    protected abstract void init(T config);

    protected abstract Class<T> configClass();

    protected T buildConfig(File configFile) {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        try {
            return mapper.readValue(configFile, configClass());
        } catch (IOException e) {
            throw new RuntimeException("Unable to read starter config file: " + configFile, e);
        }
    }

    protected IAgentHost initAgentHost(AgentHostConfig config) {
        IAgentHost agentHost = IAgentHost.newInstance(AgentHostOptions.builder()
            .addr(config.getHost())
            .port(config.getPort())
            .env(config.getEnv())
            .clusterDomainName(config.getClusterDomainName())
            .serverSslContext(config.isEnableSSL() ? buildServerSslContext(config.serverSSLContextConfig()) : null)
            .clientSslContext(config.isEnableSSL() ? buildClientSslContext(config.clientSSLContextConfig()) : null)
            .build());
        agentHost.start();
        if (!Strings.isNullOrEmpty(config.getSeedEndpoints())) {
            log.debug("AgentHost join seedEndpoints: {}", config.getSeedEndpoints());
            agentHost.join(convertSeedEndpoints(config.getSeedEndpoints()))
                .whenComplete((v, e) -> {
                    if (e != null) {
                        log.warn("AgentHost failed to join seedEndpoint: {}", config.getSeedEndpoints(), e);
                    } else {
                        log.info("AgentHost joined seedEndpoint: {}", config.getSeedEndpoints());
                    }
                });
        }
        return agentHost;
    }

    protected SslContext buildServerSslContext(ServerSSLContextConfig config) {
        if ((Strings.isNullOrEmpty(config.getCertFile()) || Strings.isNullOrEmpty(config.getKeyFile()))) {
            if (config.isSelfSigned()) {
                try {
                    SelfSignedCertificate cert = new SelfSignedCertificate();
                    return buildServerSslContext(cert.certificate(), cert.privateKey(),
                        Strings.isNullOrEmpty(config.getTrustCertsFile()) ? null :
                            loadFromConfDir(config.getTrustCertsFile()),
                        ClientAuth.valueOf(config.getClientAuth()));
                } catch (CertificateException e) {
                    throw new RuntimeException("Fail to generate self-signed certificate", e);
                }
            } else {
                throw new RuntimeException("Fail to initialize server SSLContext, no cert or key file provided");
            }
        } else {
            return buildServerSslContext(loadFromConfDir(config.getCertFile()), loadFromConfDir(config.getKeyFile()),
                Strings.isNullOrEmpty(config.getTrustCertsFile()) ? null : loadFromConfDir(config.getTrustCertsFile()),
                ClientAuth.valueOf(config.getClientAuth()));
        }
    }

    protected SslContext buildServerSslContext(File certFile, File keyFile, File trustCertsFile,
                                               ClientAuth clientAuth) {
        try {
            SslProvider sslProvider = defaultSslProvider();
            SslContextBuilder sslCtxBuilder = SslContextBuilder
                .forServer(certFile, keyFile)
                .clientAuth(clientAuth)
                .sslProvider(sslProvider);
            if (trustCertsFile == null) {
                sslCtxBuilder.trustManager(InsecureTrustManagerFactory.INSTANCE);
            } else {
                sslCtxBuilder.trustManager(trustCertsFile);
            }
            if (sslProvider == SslProvider.JDK) {
                sslCtxBuilder.sslContextProvider(findJdkProvider());
            }
            return sslCtxBuilder.build();
        } catch (Throwable e) {
            throw new RuntimeException("Fail to initialize server SSLContext", e);
        }
    }

    protected SslContext buildClientSslContext(ClientSSLContextConfig config) {
        try {
            SslProvider sslProvider = defaultSslProvider();
            SslContextBuilder sslCtxBuilder = SslContextBuilder
                .forClient()
                .trustManager(loadFromConfDir(config.getTrustCertsFile()))
                .keyManager(loadFromConfDir(config.getCertFile()), loadFromConfDir(config.getKeyFile()))
                .sslProvider(sslProvider);
            if (sslProvider == SslProvider.JDK) {
                sslCtxBuilder.sslContextProvider(findJdkProvider());
            }
            return sslCtxBuilder.build();
        } catch (Throwable e) {
            throw new RuntimeException("Fail to initialize client SSLContext", e);
        }
    }

    protected void setupMetrics() {
        new JvmInfoMetrics().bindTo(Metrics.globalRegistry);
        new JvmMemoryMetrics().bindTo(Metrics.globalRegistry);
        new JvmGcMetrics().bindTo(Metrics.globalRegistry);
        new JvmHeapPressureMetrics().bindTo(Metrics.globalRegistry);
        new JvmThreadMetrics().bindTo(Metrics.globalRegistry);
        new UptimeMetrics().bindTo(Metrics.globalRegistry);
        new ClassLoaderMetrics().bindTo(Metrics.globalRegistry);
        new ProcessorMetrics().bindTo(Metrics.globalRegistry);
    }

    private SslProvider defaultSslProvider() {
        if (OpenSsl.isAvailable()) {
            return SslProvider.OPENSSL;
        }
        Provider jdkProvider = findJdkProvider();
        if (jdkProvider != null) {
            return SslProvider.JDK;
        }
        throw new IllegalStateException("Could not find TLS provider");
    }

    private Provider findJdkProvider() {
        Provider[] providers = Security.getProviders("SSLContext.TLS");
        if (providers.length > 0) {
            return providers[0];
        }
        return null;
    }

    private Set<InetSocketAddress> convertSeedEndpoints(String endpoints) {
        return Arrays.stream(endpoints.split(","))
            .map(endpoint -> {
                String[] hostPort = endpoint.trim().split(":");
                return new InetSocketAddress(hostPort[0], Integer.parseInt(hostPort[1]));
            }).collect(Collectors.toSet());
    }

    public static File loadFromConfDir(String fileName) {
        return ResourceUtil.getFile(fileName, CONF_DIR_PROP);
    }
}
