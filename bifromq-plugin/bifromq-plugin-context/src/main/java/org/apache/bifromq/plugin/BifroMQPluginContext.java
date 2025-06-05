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

package org.apache.bifromq.plugin;

/**
 * Provides a base context for custom BifroMQ plugins. This abstract class should be subclassed by developers to create
 * a context for their specific plugin implementation. An instance of this class is created and managed by BifroMQ's
 * plugin manager during the initialization phase of the plugin lifecycle.
 *
 * <p>Subclasses may override the {@link #init()} and {@link #close()} methods to perform initialization and cleanup
 * tasks.</p>
 */
public abstract class BifroMQPluginContext {
    protected final BifroMQPluginDescriptor descriptor;

    /**
     * Constructs a new plugin context with the specified descriptor.
     *
     * @param descriptor the descriptor that defines this plugin context
     */
    public BifroMQPluginContext(BifroMQPluginDescriptor descriptor) {
        this.descriptor = descriptor;
    }

    /**
     * Initializes the plugin context. This method is called during the plugin startup sequence. The default
     * implementation does nothing and can be overridden by subclasses to provide specific behavior.
     */
    protected void init() {
        // do nothing
    }

    /**
     * Cleans up resources used by the plugin context. This method is called during the plugin shutdown sequence. The
     * default implementation does nothing and can be overridden by subclasses to provide specific cleanup behavior.
     */
    protected void close() {
        // do nothing
    }
}
