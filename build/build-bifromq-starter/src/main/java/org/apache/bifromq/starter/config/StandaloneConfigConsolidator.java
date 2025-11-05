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

package org.apache.bifromq.starter.config;

import static org.apache.bifromq.basekv.localengine.rocksdb.RocksDBDefaultConfigs.DB_CHECKPOINT_ROOT_DIR;
import static org.apache.bifromq.basekv.localengine.rocksdb.RocksDBDefaultConfigs.DB_ROOT_DIR;

import com.google.common.base.Strings;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.nio.file.Paths;
import java.security.cert.CertificateException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.basehookloader.BaseHookLoader;
import org.apache.bifromq.basekv.localengine.StructUtil;
import org.apache.bifromq.basekv.localengine.spi.IKVEngineProvider;
import org.apache.bifromq.starter.config.model.ClusterConfig;
import org.apache.bifromq.starter.config.model.EngineConfig;
import org.apache.bifromq.starter.config.model.RPCConfig;
import org.apache.bifromq.starter.config.model.SSLContextConfig;
import org.apache.bifromq.starter.config.model.ServerSSLContextConfig;
import org.apache.bifromq.starter.config.model.api.APIServerConfig;
import org.apache.bifromq.starter.config.model.dist.DistWorkerConfig;
import org.apache.bifromq.starter.config.model.inbox.InboxStoreConfig;
import org.apache.bifromq.starter.config.model.mqtt.MQTTServerConfig;
import org.apache.bifromq.starter.config.model.retain.RetainStoreConfig;

@Slf4j
public class StandaloneConfigConsolidator {
    private static final String USER_DIR_PROP = "user.dir";
    private static final String DATA_DIR_PROP = "DATA_DIR";
    private static final String DATA_PATH_ROOT = "dataPathRoot";

    public static void consolidate(StandaloneConfig config) {
        consolidateClusterConfig(config);
        consolidateMQTTServerConfig(config);
        consolidateRPCConfig(config);
        consolidateAPIServerConfig(config);
        consolidateEngineConfigs(config);
    }

    private static void consolidateEngineConfigs(StandaloneConfig config) {
        // Dist
        DistWorkerConfig distWorker = config.getDistServiceConfig().getWorker();
        if (distWorker != null) {
            consolidateEngine(distWorker.getDataEngineConfig(), true, "dist_data");
            consolidateEngine(distWorker.getWalEngineConfig(), false, "dist_wal");
        }
        // Inbox
        InboxStoreConfig inboxStore = config.getInboxServiceConfig().getStore();
        if (inboxStore != null) {
            consolidateEngine(inboxStore.getDataEngineConfig(), true, "inbox_data");
            consolidateEngine(inboxStore.getWalEngineConfig(), false, "inbox_wal");
        }
        // Retain
        RetainStoreConfig retainStore = config.getRetainServiceConfig().getStore();
        if (retainStore != null) {
            consolidateEngine(retainStore.getDataEngineConfig(), true, "retain_data");
            consolidateEngine(retainStore.getWalEngineConfig(), false, "retain_wal");
        }
    }

    private static void consolidateEngine(EngineConfig cfg, boolean cpable, String name) {
        if (cfg == null) {
            cfg = new EngineConfig();
        }
        String type = cfg.getType();
        if (Strings.isNullOrEmpty(type)) {
            type = "rocksdb";
            cfg.setType(type);
        }
        IKVEngineProvider provider = findProvider(type);
        Struct base = cpable ? provider.defaultsForCPable() : provider.defaultsForWALable();
        Map<String, Object> override = cfg;
        Map<String, Object> merged = overlay(base, override);
        derivePathsIfNeeded(cfg, merged, cpable, name);
        cfg.clear();
        cfg.setProps(merged);
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

    private static IKVEngineProvider findProvider(String type) {
        Map<String, IKVEngineProvider> providers = BaseHookLoader.load(IKVEngineProvider.class);
        IKVEngineProvider found = null;
        for (IKVEngineProvider p : providers.values()) {
            if (p.type().equalsIgnoreCase(type)) {
                if (found != null) {
                    throw new IllegalStateException("Duplicate storage engine provider type: " + type);
                }
                found = p;
            }
        }
        if (found == null) {
            throw new IllegalArgumentException("Unsupported storage engine type: " + type);
        }
        return found;
    }

    private static Map<String, Object> overlay(Struct base, Map<String, Object> override) {
        Map<String, Object> merged = new HashMap<>();
        base.getFieldsMap().forEach((k, defVal) -> {
            if (override.containsKey(k)) {
                Value newVal = StructUtil.toValue(override.get(k));
                if (defVal.getKindCase() != newVal.getKindCase()) {
                    log.warn("Invalid engine config value type: {}, required={}", newVal.getKindCase(),
                        defVal.getKindCase());
                    merged.put(k, normalizeNumber(StructUtil.fromValue(defVal)));
                } else {
                    merged.put(k, normalizeNumber(StructUtil.fromValue(newVal)));
                }
            } else {
                merged.put(k, normalizeNumber(StructUtil.fromValue(defVal)));
            }
        });
        override.forEach((k, v) -> {
            if (!merged.containsKey(k) && !k.equals(DATA_PATH_ROOT)) {
                log.warn("Unrecognized engine config: {}={}", k, v);
            }
        });
        return merged;
    }

    private static Object normalizeNumber(Object v) {
        if (v instanceof Number) {
            double d = ((Number) v).doubleValue();
            if (Double.isFinite(d)) {
                long l = (long) d;
                if (d == l) {
                    if (l >= Integer.MIN_VALUE && l <= Integer.MAX_VALUE) {
                        return (int) l;
                    }
                    return l;
                }
            }
        }
        return v;
    }

    private static void derivePathsIfNeeded(EngineConfig cfg,
                                            Map<String, Object> completeConf,
                                            boolean cpable,
                                            String name) {
        if (!"rocksdb".equalsIgnoreCase(cfg.getType())) {
            return;
        }
        String dataRootPath;
        if (cfg.containsKey(DATA_PATH_ROOT)) {
            // fill back data path root
            completeConf.put(DATA_PATH_ROOT, cfg.get(DATA_PATH_ROOT));
            if (Paths.get((String) cfg.get(DATA_PATH_ROOT)).isAbsolute()) {
                dataRootPath = (String) cfg.get(DATA_PATH_ROOT);
            } else {
                String userDir = System.getProperty(USER_DIR_PROP);
                String dataDir = System.getProperty(DATA_DIR_PROP, userDir);
                dataRootPath = Paths.get(dataDir, (String) cfg.get(DATA_PATH_ROOT)).toAbsolutePath()
                    .toString();
            }
        } else {
            String userDir = System.getProperty(USER_DIR_PROP);
            String dataDir = System.getProperty(DATA_DIR_PROP, userDir);
            dataRootPath = Paths.get(dataDir).toAbsolutePath().toString();
        }
        completeConf.put(DB_ROOT_DIR, Paths.get(dataRootPath, name).toString());
        if (cpable) {
            completeConf.put(DB_CHECKPOINT_ROOT_DIR, Paths.get(dataRootPath, name + "_cp").toString());
        }
    }
}
