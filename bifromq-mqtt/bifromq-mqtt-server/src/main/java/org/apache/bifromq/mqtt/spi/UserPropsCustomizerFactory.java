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

package org.apache.bifromq.mqtt.spi;

import com.google.protobuf.Struct;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.basehookloader.BaseHookLoader;

@Slf4j
public class UserPropsCustomizerFactory implements IUserPropsCustomizerFactory {
    private final List<IUserPropsCustomizerFactory> factories = new ArrayList<>();

    /**
     * Creates a UserPropsCustomizerFactory with the provided configuration map.
     *
     * @param configMap A map where the key is the factory name and the value is the configuration for that factory.
     */
    public UserPropsCustomizerFactory(Map<String, Struct> configMap) {
        Map<String, IUserPropsCustomizerFactory> loadedFactories =
            BaseHookLoader.load(IUserPropsCustomizerFactory.class);
        for (String factoryName : loadedFactories.keySet()) {
            IUserPropsCustomizerFactory fac = loadedFactories.get(factoryName);
            Struct config = configMap.getOrDefault(factoryName, Struct.getDefaultInstance());
            try {
                fac.init(config);
                log.info("UserPropsCustomizerFactory {} Loaded", factoryName);
                factories.add(fac);
            } catch (Exception e) {
                log.error("Failed to initialize UserPropsCustomizerFactory {}", factoryName, e);
            }
        }
    }

    @Override
    public IUserPropsCustomizer create() {
        List<IUserPropsCustomizer> customizers = new ArrayList<>();
        for (IUserPropsCustomizerFactory factory : factories) {
            try {
                IUserPropsCustomizer customizer = factory.create();
                customizers.add(customizer);
            } catch (Exception e) {
                log.error("Failed to create UserPropsCustomizer from factory {}", factory.getClass().getName(), e);
            }
        }
        return new UserPropsCustomizer(customizers);
    }

    @Override
    public void close() {
        for (IUserPropsCustomizerFactory factory : factories) {
            try {
                factory.close();
            } catch (Exception e) {
                log.error("Failed to close UserPropsCustomizerFactory {}", factory.getClass().getName(), e);
            }
        }
        factories.clear();
        log.info("All UserPropsCustomizerFactories closed");
    }
}
