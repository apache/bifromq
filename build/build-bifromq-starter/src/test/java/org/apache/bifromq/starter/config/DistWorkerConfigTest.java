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

import static org.testng.Assert.assertEquals;

import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.bifromq.starter.utils.ConfigFileUtil;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class DistWorkerConfigTest {
    @DataProvider
    public Object[][] intervals() {
        return new Object[][] {
            {"{}", 0, 32},
            {"distServiceConfig:\n  worker:\n    minGCIntervalSeconds: 30\n    maxGCIntervalSeconds: 86400\n", 30, 86400}
        };
    }

    @Test(dataProvider = "intervals")
    public void gcUsesDefaultsUnlessExplicitlyConfigured(String yaml, int min, int max) throws Exception {
        Path file = Files.createTempFile("dist-gc-config", ".yaml");
        try {
            Files.writeString(file, yaml);
            StandaloneConfig config = ConfigFileUtil.build(file.toFile(), StandaloneConfig.class);
            assertEquals(config.getDistServiceConfig().getWorker().getMinGCIntervalSeconds(), min);
            assertEquals(config.getDistServiceConfig().getWorker().getMaxGCIntervalSeconds(), max);
            assertEquals(config.getInboxServiceConfig().getStore().getMinGCIntervalSeconds(), 30);
            assertEquals(config.getInboxServiceConfig().getStore().getMaxGCIntervalSeconds(), 86400);
        } finally {
            Files.delete(file);
        }
    }
}
