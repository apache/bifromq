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

package org.apache.bifromq.basekv.localengine;

import java.lang.reflect.Method;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

@Slf4j
public abstract class MockableTest {
    private AutoCloseable closeable;

    @BeforeMethod(alwaysRun = true)
    public final void setup(Method method) {
        log.info("Test case[{}] start @ {}", method.getName(), this.getClass().getSimpleName());
        closeable = MockitoAnnotations.openMocks(this);
        doSetup(method);
    }

    protected void doSetup(Method method) {
    }

    @SneakyThrows
    @AfterMethod(alwaysRun = true)
    public final void teardown(Method method) {
        log.info("Test case[{}] finished, doing teardown", method.getName());
        try {
            doTeardown(method);
            log.info("Test case[{}] teared down", method.getName());
        } catch (Throwable e) {
            log.warn("Test case[{}] teardown exception", method.getName(), e);
        }
        closeable.close();
    }

    protected void doTeardown(Method method) {
    }
}
