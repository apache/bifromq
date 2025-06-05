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

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.mockito.Mock;
import org.testng.annotations.Test;

public class SyncContextTest extends MockableTest {

    @Mock
    private Runnable refresh;

    @Mock
    private Runnable mutate;

    @Test
    public void refreshAsNeeded() {
        SyncContext accessor = new SyncContext();
        ISyncContext.IRefresher refresher1 = accessor.refresher();
        ISyncContext.IRefresher refresher2 = accessor.refresher();
        ISyncContext.IMutator mutator = accessor.mutator();

        refresher1.runIfNeeded(refresh);
        refresher2.runIfNeeded(refresh);
        verify(refresh, times(2)).run();

        mutator.run(mutate);
        verify(mutate, times(1)).run();

        refresher1.runIfNeeded(refresh);
        refresher2.runIfNeeded(refresh);
        verify(refresh, times(4)).run();

        refresher1.runIfNeeded(refresh);
        refresher2.runIfNeeded(refresh);
        verify(refresh, times(4)).run();
    }
}
