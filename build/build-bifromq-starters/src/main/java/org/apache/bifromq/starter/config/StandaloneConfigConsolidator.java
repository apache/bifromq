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

package org.apache.bifromq.starter.config;

import org.apache.bifromq.starter.config.model.ClusterConfig;
import org.apache.bifromq.starter.config.model.RPCConfig;
import org.apache.bifromq.starter.config.model.SSLContextConfig;
import org.apache.bifromq.starter.config.model.ServerSSLContextConfig;
import org.apache.bifromq.starter.config.model.api.APIServerConfig;
import org.apache.bifromq.starter.config.model.mqtt.MQTTServerConfig;
import com.google.common.base.Strings;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.security.cert.CertificateException;
import java.util.Enumeration;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StandaloneConfigConsolidator {

    public static void consolidate(StandaloneConfig config) {
        consolidateClusterConfig(config);
        consolidateMQTTServerConfig(config);
        consolidateRPCConfig(config);
        consolidateAPIServerConfig(config);
    }

    private static void consolidateClusterConfig(StandaloneConfig config) {
        ClusterConfig clusterConfig = config.getClusterConfig();
        if (Strings.isNullOrEmpty(clusterConfig.getHost())) {
            clusterConfig.setHost(resolveHost(config));
        }
        if (Strings.isNullOrEmpty(clusterConfig.getEnv())) {
            throw new IllegalArgumentException("Cluster env cannot be null or empty string");
        }
        if (!Strings.isNullOrEmpty(clusterConfig.getClusterDomainName()) && clusterConfig.getPort() == 0) {
            throw new IllegalArgumentException(
                "Port number must be specified and make sure all members use same number if seed address is resolved from domain name");
        }
    }

    private static void consolidateMQTTServerConfig(StandaloneConfig config) {
        MQTTServerConfig mqttServerConfig = config.getMqttServiceConfig().getServer();
        // fill default host
        if (mqttServerConfig.getTcpListener().getHost() == null) {
            mqttServerConfig.getTcpListener().setHost("0.0.0.0");
        }
        if (mqttServerConfig.getTlsListener().getHost() == null) {
            mqttServerConfig.getTlsListener().setHost(mqttServerConfig.getTcpListener().getHost());
        }
        if (mqttServerConfig.getWsListener().getHost() == null) {
            mqttServerConfig.getWsListener().setHost(mqttServerConfig.getTcpListener().getHost());
        }
        if (mqttServerConfig.getWssListener().getHost() == null) {
            mqttServerConfig.getWssListener().setHost(mqttServerConfig.getTcpListener().getHost());
        }
        // fill self-signed certificate for ssl connection
        if ((mqttServerConfig.getWssListener().isEnable()
            && mqttServerConfig.getWssListener().getSslConfig() == null)
            || (mqttServerConfig.getTlsListener().isEnable()
            && mqttServerConfig.getTlsListener().getSslConfig() == null)) {
            try {
                ServerSSLContextConfig sslContextConfig = genSelfSignedServerCert();
                if (mqttServerConfig.getTlsListener().isEnable()
                    && mqttServerConfig.getTlsListener().getSslConfig() == null) {
                    mqttServerConfig.getTlsListener().setSslConfig(sslContextConfig);
                }
                if (mqttServerConfig.getWssListener().isEnable()
                    && mqttServerConfig.getWssListener().getSslConfig() == null) {
                    mqttServerConfig.getWssListener().setSslConfig(sslContextConfig);
                }
            } catch (Throwable e) {
                log.warn("Unable to generate self-signed certificate, mqtt over tls or wss will be disabled", e);
                if (mqttServerConfig.getTlsListener().isEnable()
                    && mqttServerConfig.getTlsListener().getSslConfig() == null) {
                    mqttServerConfig.getTlsListener().setEnable(false);
                }
                if (mqttServerConfig.getWssListener().isEnable()
                    && mqttServerConfig.getWssListener().getSslConfig() == null) {
                    mqttServerConfig.getWssListener().setEnable(false);
                }
            }
        }
    }

    private static void consolidateRPCConfig(StandaloneConfig config) {
        // fill default host
        RPCConfig rpcConfig = config.getRpcConfig();
        if (rpcConfig.getHost() == null) {
            rpcConfig.setHost(resolveHost(config));
        }
        if (rpcConfig.isEnableSSL()) {
            if (rpcConfig.getClientSSLConfig() == null
                && rpcConfig.getServerSSLConfig() == null) {
                rpcConfig.setClientSSLConfig(new SSLContextConfig());
                try {
                    log.warn("Generate self-signed certificate for RPC server");
                    rpcConfig.setServerSSLConfig(genSelfSignedServerCert());
                } catch (Throwable e) {
                    log.warn("Unable to generate self-signed certificate, rpc client ssl will be disabled", e);
                    rpcConfig.setEnableSSL(false);
                    rpcConfig.setClientSSLConfig(null);
                    rpcConfig.setServerSSLConfig(null);
                }
            }
        }
    }

    private static void consolidateAPIServerConfig(StandaloneConfig config) {
        APIServerConfig apiServerConfig = config.getApiServerConfig();
        if (apiServerConfig.getHost() == null) {
            apiServerConfig.setHost(resolveHost(config));
        }
        // fill self-signed certificate for ssl connection
        if (apiServerConfig.isEnableSSL()
            && apiServerConfig.getSslConfig() == null) {
            try {
                apiServerConfig.setSslConfig(genSelfSignedServerCert());
            } catch (Throwable e) {
                log.warn("Unable to generate self-signed certificate, Https API listener is be disabled", e);
                apiServerConfig.setEnableSSL(false);
            }
        }
    }

    @SneakyThrows
    private static String resolveHost(StandaloneConfig config) {
        String host = config.getMqttServiceConfig().getServer().getTcpListener().getHost();
        if (!"0.0.0.0".equals(host)) {
            return host;
        }
        host = config.getRpcConfig().getHost();
        if (!Strings.isNullOrEmpty(host)) {
            return host;
        }
        host = config.getClusterConfig().getHost();
        if (!Strings.isNullOrEmpty(host)) {
            return host;
        }
        Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
        while (networkInterfaces.hasMoreElements()) {
            NetworkInterface networkInterface = networkInterfaces.nextElement();
            Enumeration<InetAddress> inetAddresses = networkInterface.getInetAddresses();
            while (inetAddresses.hasMoreElements()) {
                InetAddress inetAddress = inetAddresses.nextElement();
                if (!inetAddress.isLoopbackAddress()
                    && !inetAddress.isLinkLocalAddress()
                    && inetAddress.isSiteLocalAddress()) {
                    return inetAddress.getHostAddress();
                }
            }
        }
        throw new IllegalStateException("Unable to resolve host, please specify host in config file");
    }

    private static ServerSSLContextConfig genSelfSignedServerCert() throws CertificateException {
        SelfSignedCertificate selfCert = new SelfSignedCertificate();
        ServerSSLContextConfig sslContextConfig = new ServerSSLContextConfig();
        sslContextConfig.setCertFile(selfCert.certificate().getAbsolutePath());
        sslContextConfig.setKeyFile(selfCert.privateKey().getAbsolutePath());
        return sslContextConfig;
    }
}
