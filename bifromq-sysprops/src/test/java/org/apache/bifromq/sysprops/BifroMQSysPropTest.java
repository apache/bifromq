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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.apache.bifromq.sysprops.props.ClusterDomainResolveTimeoutSeconds;
import java.util.HashSet;
import java.util.Set;
import org.reflections.Reflections;
import org.testng.annotations.Test;

public class BifroMQSysPropTest {

    @Test
    public void propKeyNoConflict() {
        Reflections reflections = new Reflections(BifroMQSysProp.class.getPackageName());
        Set<String> propKeys = new HashSet<>();
        for (Class<? extends BifroMQSysProp> subclass : reflections.getSubTypesOf(BifroMQSysProp.class)) {
            try {
                // Assuming each subclass has a public static INSTANCE field
                BifroMQSysProp<?, ?> instance = (BifroMQSysProp<?, ?>) subclass.getField("INSTANCE").get(null);
                String propKey = instance.propKey();
                assertTrue(propKeys.add(propKey), "Duplicate propKey found: " + propKey);
            } catch (NoSuchFieldException | IllegalAccessException e) {
                throw new RuntimeException("Failed to access INSTANCE field of subclass: " + subclass.getName(), e);
            }
        }
    }

    @Test
    public void testResolve() {
        ClusterDomainResolveTimeoutSeconds.INSTANCE.resolve();
        assertEquals(ClusterDomainResolveTimeoutSeconds.INSTANCE.get(), Long.valueOf(120));

        System.setProperty(ClusterDomainResolveTimeoutSeconds.INSTANCE.propKey(), "100");
        ClusterDomainResolveTimeoutSeconds.INSTANCE.resolve();
        assertEquals(ClusterDomainResolveTimeoutSeconds.INSTANCE.get(), Long.valueOf(100));
    }
}
