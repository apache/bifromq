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

/**
 * Factory interface for creating instances of {@link IUserPropsCustomizer}.
 * Implementations should provide a no-argument constructor.
 * <br>
 * The factory can be initialized with a configuration Struct and closed when no longer needed.
 */
public interface IUserPropsCustomizerFactory {
    /**
     * Creates a new instance of {@link IUserPropsCustomizer}.
     *
     * @return A new instance of {@link IUserPropsCustomizer}.
     */
    IUserPropsCustomizer create();

    /**
     * Initializes the factory with the given configuration.
     *
     * @param config The configuration Struct defined in config file.
     */
    default void init(Struct config) {
    }

    /**
     * Closes the factory and releases any resources it holds.
     */
    default void close() {

    }
}
