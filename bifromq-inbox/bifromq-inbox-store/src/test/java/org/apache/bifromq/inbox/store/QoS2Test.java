/*
 * Copyright (c) 2023. The BifroMQ Authors. All Rights Reserved.
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

package org.apache.bifromq.inbox.store;

import static org.apache.bifromq.type.QoS.EXACTLY_ONCE;

import org.testng.annotations.Test;

public class QoS2Test extends InboxInsertTest {
    @Test(groups = "integration")
    public void fetchWithoutStartAfter() {
        fetchWithoutStartAfter(EXACTLY_ONCE);
    }

    @Test(groups = "integration")
    public void fetchWithMaxLimit() {
        fetchWithMaxLimit(EXACTLY_ONCE);
    }

    @Test(groups = "integration")
    public void fetchWithStartAfter() {
        fetchWithStartAfter(EXACTLY_ONCE);
    }

    @Test(groups = "integration")
    public void commit() {
        commit(EXACTLY_ONCE);
    }

    @Test(groups = "integration")
    public void commitAll() {
        commitAll(EXACTLY_ONCE);
    }

    @Test(groups = "integration")
    public void insertDropYoungest() {
        insertDropYoungest(EXACTLY_ONCE);
    }
}

