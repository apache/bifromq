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

package org.apache.bifromq.starter.utils;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.bifromq.starter.config.StandaloneConfig;
import org.apache.bifromq.starter.config.model.BalancerOptions;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ConfigFileUtilTest {
    @Test
    public void readsKnownOptionsAndKeepsDefaultsAndUnknownProperties() throws Exception {
        StandaloneConfig config = read("rpcConfig:\n  port: 19090\nunknownConfig: true\n",
            StandaloneConfig.class);
        assertEquals(config.getRpcConfig().getPort(), 19090);
        assertTrue(config.getRpcConfig().getClientEventLoopThreads() >= 4);
        assertEquals(config.getInboxServiceConfig().getStore().getMinGCIntervalSeconds(), 30);
    }

    @Test
    public void roundTripsBalancerStructValues() throws Exception {
        BalancerOptions options = new BalancerOptions();
        Struct values = Struct.newBuilder()
            .putFields("target", Value.newBuilder().setNumberValue(3).build())
            .putFields("enabled", Value.newBuilder().setBoolValue(true).build())
            .putFields("name", Value.newBuilder().setStringValue("compat").build()).build();
        options.getBalancers().put("example.Balancer", values);
        BalancerOptions decoded = read(ConfigFileUtil.serialize(options), BalancerOptions.class);
        assertEquals(decoded.getBalancers(), options.getBalancers());
        assertEquals(decoded.getBootstrapDelayInMS(), options.getBootstrapDelayInMS());
    }

    @DataProvider
    public Object[][] invalidYaml() {
        return new Object[][] {
            {"rpcConfig:\n  port: 1\n  port: 2\n"},
            {"rpcConfig: [\n"}
        };
    }

    @Test(dataProvider = "invalidYaml")
    public void rejectsDuplicateAndMalformedYaml(String yaml) throws Exception {
        expectThrows(RuntimeException.class, () -> read(yaml, StandaloneConfig.class));
    }

    private static <T> T read(String yaml, Class<T> type) throws Exception {
        Path path = Files.createTempFile("config-compat", ".yaml");
        try {
            Files.writeString(path, yaml);
            return ConfigFileUtil.build(path.toFile(), type);
        } finally {
            Files.delete(path);
        }
    }
}
