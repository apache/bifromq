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

package org.apache.bifromq.plugin;

import java.nio.file.Path;
import lombok.Getter;
import lombok.ToString;
import org.pf4j.PluginDescriptor;

/**
 * The descriptor of BifroMQPlugin passed by BifroMQPluginManager during plugin initialization.
 */
@Getter
@ToString
public class BifroMQPluginDescriptor {
    private final PluginDescriptor descriptor;
    private final Path pluginRoot;
    private final ClassLoader pluginClassLoader;
    private final boolean isDevelopment;

    /**
     * Constructor of BifroMQPluginContext.
     *
     * @param descriptor    the descriptor of the plugin
     * @param pluginRoot    the root path of the plugin
     * @param classLoader   the plugin specific classloader
     * @param isDevelopment the runtime mode
     */
    public BifroMQPluginDescriptor(PluginDescriptor descriptor,
                                   Path pluginRoot,
                                   ClassLoader classLoader,
                                   boolean isDevelopment) {
        this.descriptor = descriptor;
        this.pluginRoot = pluginRoot;
        this.pluginClassLoader = classLoader;
        this.isDevelopment = isDevelopment;
    }
}
