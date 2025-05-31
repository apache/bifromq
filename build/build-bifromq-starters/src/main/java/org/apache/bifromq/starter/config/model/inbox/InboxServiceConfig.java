/*
 * Copyright (c) 2024. The BifroMQ Authors. All Rights Reserved.
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

package org.apache.bifromq.starter.config.model.inbox;

import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class InboxServiceConfig {
    @JsonSetter(nulls = Nulls.SKIP)
    private InboxClientConfig client = new InboxClientConfig();

    @JsonSetter(nulls = Nulls.SKIP)
    private InboxServerConfig server = new InboxServerConfig();

    @JsonSetter(nulls = Nulls.SKIP)
    private InboxStoreClientConfig storeClient = new InboxStoreClientConfig();

    @JsonSetter(nulls = Nulls.SKIP)
    private InboxStoreConfig store = new InboxStoreConfig();
}
