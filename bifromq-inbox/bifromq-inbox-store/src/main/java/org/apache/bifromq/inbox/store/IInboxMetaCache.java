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

import java.util.Optional;
import java.util.SortedMap;
import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.store.api.IKVIterator;
import org.apache.bifromq.inbox.storage.proto.InboxMetadata;

public interface IInboxMetaCache {

    SortedMap<Long, InboxMetadata> get(String tenantId, String inboxId, IKVIterator itr);

    Optional<InboxMetadata> get(String tenantId, String inboxId, long incarnation, IKVIterator itr);

    boolean upsert(String tenantId, InboxMetadata metadata, IKVIterator itr);

    boolean remove(String tenantId, String inboxId, long incarnation, IKVIterator itr);

    void reset(Boundary boundary);

    void close();
}


