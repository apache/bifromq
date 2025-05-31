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

import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.pf4j.Plugin;
import org.pf4j.PluginWrapper;

/**
 * Base class for custom BifroMQ plugins. This abstract class provides the necessary infrastructure to manage the
 * lifecycle of a plugin, including initialization and shutdown. It utilizes generics to ensure that each plugin works
 * with its specific type of BifroMQPluginContext.
 *
 * @param <C> the type of the plugin context that this plugin uses. Must extend BifroMQPluginContext.
 */
@Slf4j
public abstract class BifroMQPlugin<C extends BifroMQPluginContext> extends Plugin {

    protected final BifroMQPluginDescriptor descriptor;
    private final C context;

    /**
     * Constructs a new BifroMQPlugin with the specified descriptor. This constructor initializes the plugin context
     * specific to the derived plugin class.
     *
     * @param descriptor the descriptor passed by the plugin manager, containing metadata and configuration.
     */
    @SneakyThrows
    protected BifroMQPlugin(BifroMQPluginDescriptor descriptor) {
        this.descriptor = descriptor;
        this.context = createContextInstance(descriptor);
    }

    private BifroMQPlugin(PluginWrapper wrapper) {
        throw new UnsupportedOperationException("Deprecated constructor is not allowed");
    }

    // hide the no-arg constructor
    private BifroMQPlugin() {
        throw new UnsupportedOperationException("No-arg constructor is not allowed");
    }

    @SneakyThrows
    private C createContextInstance(BifroMQPluginDescriptor descriptor) {
        Type genericSuperclass = getClass().getGenericSuperclass();
        if (genericSuperclass instanceof ParameterizedType parameterizedType) {
            Type[] typeArguments = parameterizedType.getActualTypeArguments();
            if (typeArguments.length > 0) {
                @SuppressWarnings("unchecked")
                Class<C> contextClass = (Class<C>) typeArguments[0];
                Constructor<C> constructor = contextClass.getDeclaredConstructor(BifroMQPluginDescriptor.class);
                // Check constructor visibility
                int modifiers = constructor.getModifiers();
                if (Modifier.isPrivate(modifiers)) {
                    throw new IllegalAccessException("Private constructor is not accessible: " + constructor.getName());
                }
                if (!Modifier.isPublic(modifiers)) {
                    log.warn("PluginContext's constructor should be public, visibility is {}: {}",
                        Modifier.isProtected(modifiers) ? "protected" : "package-private", constructor.getName());
                }
                constructor.setAccessible(true); // Allow protected and package-private access
                return constructor.newInstance(descriptor);
            }
        }
        throw new IllegalStateException("Unable to determine the context class type.");
    }

    /**
     * Provides access to the plugin's context.
     *
     * @return the context associated with this plugin.
     */
    public C context() {
        return context;
    }

    /**
     * Starts the plugin. This method is called when the plugin is loaded. It initializes the plugin context.
     */
    @Override
    public final void start() {
        ClassLoader origClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(descriptor.getPluginClassLoader());
            super.start();
            context.init();
            doStart();
        } finally {
            Thread.currentThread().setContextClassLoader(origClassLoader);
        }
    }

    /**
     * Subclasses can override this method to perform additional initialization when the plugin is started.
     */
    protected void doStart() {

    }

    /**
     * Stops the plugin. This method is called when the plugin is unloaded. It cleans up the plugin context.
     */
    @Override
    public final void stop() {
        ClassLoader origClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(descriptor.getPluginClassLoader());
            doStop();
            context.close();
            super.stop();
        } finally {
            Thread.currentThread().setContextClassLoader(origClassLoader);
        }
    }

    /**
     * Subclasses can override this method to perform additional cleanup when the plugin is stopped.
     */
    protected void doStop() {

    }

    @Override
    public final void delete() {
        super.delete();
    }
}
