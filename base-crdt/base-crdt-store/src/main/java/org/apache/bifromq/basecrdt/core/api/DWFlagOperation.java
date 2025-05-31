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

package org.apache.bifromq.basecrdt.core.api;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.ToString;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
@ToString
public final class DWFlagOperation implements ICRDTOperation {
    private static final DWFlagOperation DISABLE = new DWFlagOperation(Type.Disable);
    private static final DWFlagOperation ENABLE = new DWFlagOperation(Type.Enable);

    public enum Type {
        Disable, Enable
    }

    public final Type type;

    public static DWFlagOperation disable() {
        return DISABLE;
    }

    public static DWFlagOperation enable() {
        return ENABLE;
    }
}
