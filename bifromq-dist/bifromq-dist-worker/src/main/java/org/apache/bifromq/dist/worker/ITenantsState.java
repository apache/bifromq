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

package org.apache.bifromq.dist.worker;

public interface ITenantsState {
    void incNormalRoutes(String tenantId);

    void incNormalRoutes(String tenantId, int count);

    void decNormalRoutes(String tenantId);

    void decNormalRoutes(String tenantId, int count);

    void incSharedRoutes(String tenantId);

    void incSharedRoutes(String tenantId, int count);

    void decSharedRoutes(String tenantId);

    void decSharedRoutes(String tenantId, int count);

    void reset();

    void close();
}
