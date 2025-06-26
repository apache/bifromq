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

package org.apache.bifromq.starter.module;

import static org.apache.bifromq.starter.utils.ResourceUtil.loadFile;

import com.google.common.base.Strings;
import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import java.security.Provider;
import java.security.Security;
import org.apache.bifromq.starter.config.model.ServerSSLContextConfig;

public class SSLUtil {
    public static SslProvider defaultSslProvider() {
        if (OpenSsl.isAvailable()) {
            return SslProvider.OPENSSL;
        }
        Provider jdkProvider = findJdkProvider();
        if (jdkProvider != null) {
            return SslProvider.JDK;
        }
        throw new IllegalStateException("Could not find TLS provider");
    }

    public static Provider findJdkProvider() {
        Provider[] providers = Security.getProviders("SSLContext.TLS");
        if (providers.length > 0) {
            return providers[0];
        }
        return null;
    }

    public static SslContext buildServerSslContext(ServerSSLContextConfig config) {
        try {
            SslProvider sslProvider = defaultSslProvider();
            SslContextBuilder sslCtxBuilder = SslContextBuilder
                .forServer(loadFile(config.getCertFile()), loadFile(config.getKeyFile()))
                .clientAuth(config.getClientAuth())
                .sslProvider(sslProvider);
            if (Strings.isNullOrEmpty(config.getTrustCertsFile())) {
                sslCtxBuilder.trustManager(InsecureTrustManagerFactory.INSTANCE);
            } else {
                sslCtxBuilder.trustManager(loadFile(config.getTrustCertsFile()));
            }
            if (sslProvider == SslProvider.JDK) {
                sslCtxBuilder.sslContextProvider(findJdkProvider());
            }
            return sslCtxBuilder.build();
        } catch (Throwable e) {
            throw new RuntimeException("Fail to initialize server SSLContext", e);
        }
    }
}
