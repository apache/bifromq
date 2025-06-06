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

package ${package};

import org.apache.bifromq.plugin.BifroMQPlugin;
import org.apache.bifromq.plugin.BifroMQPluginDescriptor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;

public class ${pluginName} extends BifroMQPlugin<${pluginContextName}> {
    private static final Logger log = LoggerFactory.getLogger(${pluginName}.class);
    private static final String LOG4J2_CONFIG_FILE = "conf/log4j2.xml";
    private static final String PLUGIN_CONFIG_FILE = "conf/config.yaml";


    public ${pluginName} (BifroMQPluginDescriptor descriptor){
        super(descriptor);
        // setup logger context using plugin's log4j2.xml
        configureLoggerContext(descriptor.getPluginRoot());
        try {
            log.info("TODO: Initialize your plugin using config: {}", findConfigFile(descriptor.getPluginRoot()));
            log.info("---config.yaml start---");
            for (String line : Files.readAllLines(findConfigFile(descriptor.getPluginRoot()))) {
                log.info("{}", line);
            }
            log.info("---config.yaml end---");
        } catch (Exception e) {
            log.error("Failed to initialize plugin", e);
        }
    }

    protected void doStart() {
        log.info("TODO: Start your plugin");
    }

    protected void doStop() {
        log.info("TODO: Stop your plugin");
    }

    private void configureLoggerContext(Path rootPath) {
        try {
            Path configPath = rootPath.resolve(LOG4J2_CONFIG_FILE).toAbsolutePath();
            File log4jConfig = configPath.toFile();
            if (log4jConfig.exists()) {
                // Shutdown any existing context to avoid resource leaks
                LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
                ctx.stop();

                // Initialize a new LoggerContext with the plugin-specific configuration
                Configurator.initialize(null, configPath.toString());
                log.info("Loaded Log4j2 configuration from {}", configPath);
            } else {
                log.warn("log4j2.xml not found for {}", getClass().getName());
            }
        } catch (Exception e) {
            log.error("Failed to configure Log4j2 for {}", getClass().getName(), e);
        }
    }

    private Path findConfigFile(Path rootPath) {
        return rootPath.resolve(PLUGIN_CONFIG_FILE);
    }
}