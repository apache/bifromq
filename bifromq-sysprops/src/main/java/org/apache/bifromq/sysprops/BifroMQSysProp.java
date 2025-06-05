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

package org.apache.bifromq.sysprops;

import org.apache.bifromq.sysprops.parser.PropParser;
import lombok.extern.slf4j.Slf4j;

/**
 * The base class for the System Properties used in BifroMQ.
 *
 * @param <T> the value type of the system property after parsing
 * @param <P> the parser type for the system property
 */
@Slf4j
public abstract class BifroMQSysProp<T, P extends PropParser<T>> {
    private final String propKey;
    private final P parser;
    private final T defaultValue;
    private T currentValue;

    protected BifroMQSysProp(String propKey, T defaultValue, P parser) {
        this.propKey = propKey;
        this.defaultValue = defaultValue;
        this.parser = parser;
        resolve();
    }

    private String sysPropValue(final String key) {
        String value = null;
        try {
            value = System.getProperty(key);
        } catch (SecurityException e) {
            log.warn("Failed to retrieve a system property '{}'", key, e);
        }
        return value;
    }

    /**
     * Resolve the system property value again.
     */
    public final synchronized void resolve() {
        String value = sysPropValue(propKey);
        if (value == null) {
            currentValue = defaultValue;
            return;
        }
        value = value.trim().toLowerCase();
        if (value.isEmpty()) {
            currentValue = defaultValue;
            return;
        }
        try {
            currentValue = parser.parse(value);
        } catch (Throwable e) {
            log.warn("Failed to parse system prop '{}':{} - using the default value: {}", propKey, value, defaultValue);
            currentValue = defaultValue;
        }
    }

    /**
     * The system property key.
     *
     * @return the system property key
     */
    public final String propKey() {
        return propKey;
    }

    /**
     * Get the system property value for specific property.
     *
     * @return the system property value
     */
    public final T get() {
        return currentValue;
    }
}
