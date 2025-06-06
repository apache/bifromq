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

import org.apache.bifromq.plugin.BifroMQPluginContext;
import org.apache.bifromq.plugin.BifroMQPluginDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ${pluginContextName} extends BifroMQPluginContext{
    private static final Logger log = LoggerFactory.getLogger(${pluginContextName}.class);

    public ${pluginContextName} (BifroMQPluginDescriptor descriptor){
        super(descriptor);
    }

    @Override
    protected void init() {
        log.info("TODO: Initialize your plugin context using descriptor {}", descriptor);
    }

    @Override
    protected void close() {
        log.info("TODO: Close your plugin context");
    }
}