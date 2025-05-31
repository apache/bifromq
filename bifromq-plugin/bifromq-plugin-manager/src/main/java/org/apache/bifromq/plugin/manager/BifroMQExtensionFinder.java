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

package org.apache.bifromq.plugin.manager;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.pf4j.AbstractExtensionFinder;
import org.pf4j.PluginManager;
import org.pf4j.PluginWrapper;
import org.pf4j.processor.ExtensionStorage;
import org.pf4j.processor.LegacyExtensionStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a MOD version of LegacyExtensionFinder which scans extensions from all jars in plugin's classpath.
 */
public class BifroMQExtensionFinder extends AbstractExtensionFinder {
    private static final Logger log = LoggerFactory.getLogger(BifroMQExtensionFinder.class);

    public static final String EXTENSIONS_RESOURCE = LegacyExtensionStorage.EXTENSIONS_RESOURCE;

    protected BifroMQExtensionFinder(PluginManager pluginManager) {
        super(pluginManager);
    }

    @Override
    public Map<String, Set<String>> readPluginsStorages() {
        log.debug("Reading extensions storages from classpath");
        Map<String, Set<String>> result = new LinkedHashMap<>();
        result.put(null, collectExtensions(getClass().getClassLoader()));
        return result;
    }

    @Override
    public Map<String, Set<String>> readClasspathStorages() {
        log.debug("Reading extensions storages from plugins");
        Map<String, Set<String>> result = new LinkedHashMap<>();
        List<PluginWrapper> plugins = pluginManager.getPlugins();
        for (PluginWrapper plugin : plugins) {
            String pluginId = plugin.getDescriptor().getPluginId();
            log.debug("Reading extensions storage from plugin '{}'", pluginId);
            ClassLoader pluginClassLoader = plugin.getPluginClassLoader();
            result.put(pluginId, collectExtensions(pluginClassLoader));
        }
        return result;
    }

    private Set<String> collectExtensions(ClassLoader classLoader) {
        Set<String> bucket = new HashSet<>();
        try {
            Enumeration<URL> urls = classLoader.getResources(EXTENSIONS_RESOURCE);
            if (urls.hasMoreElements()) {
                collectExtensions(urls, bucket);
            } else {
                log.debug("Cannot find '{}'", EXTENSIONS_RESOURCE);
            }
            debugExtensions(bucket);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
        return bucket;
    }


    private void collectExtensions(Enumeration<URL> urls, Set<String> bucket) throws IOException {
        while (urls.hasMoreElements()) {
            URL url = urls.nextElement();
            log.debug("Read '{}'", url.getFile());
            collectExtensions(url.openStream(), bucket);
        }
    }

    private void collectExtensions(InputStream inputStream, Set<String> bucket) throws IOException {
        try (Reader reader = new InputStreamReader(inputStream, StandardCharsets.UTF_8)) {
            ExtensionStorage.read(reader, bucket);
        }
    }
}
