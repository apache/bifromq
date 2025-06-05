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

package org.apache.bifromq.inbox.store;

import static org.apache.bifromq.type.QoS.AT_LEAST_ONCE;

import org.testng.annotations.Test;

public class QoS1Test extends InboxInsertTest {
    @Test(groups = "integration")
    public void fetchWithoutStartAfter() {
        fetchWithoutStartAfter(AT_LEAST_ONCE);
    }

    @Test(groups = "integration")
    public void fetchWithMaxLimit() {
        fetchWithMaxLimit(AT_LEAST_ONCE);
    }

    @Test(groups = "integration")
    public void fetchWithStartAfter() {
        fetchWithStartAfter(AT_LEAST_ONCE);
    }

    @Test(groups = "integration")
    public void commit() {
        commit(AT_LEAST_ONCE);
    }

    @Test(groups = "integration")
    public void commitAll() {
        commitAll(AT_LEAST_ONCE);
    }

    @Test(groups = "integration")
    public void insertDropYoungest() {
        insertDropYoungest(AT_LEAST_ONCE);
    }
}

