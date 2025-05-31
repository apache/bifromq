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

import com.google.protobuf.ByteString;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.ToString;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
@ToString
public final class MVRegOperation implements ICRDTOperation {
    private static final MVRegOperation RESET = new MVRegOperation(Type.Reset, null);

    public enum Type {
        Write, Reset
    }

    public final Type type;
    public final ByteString value;

    public static MVRegOperation write(ByteString val) {
        return new MVRegOperation(Type.Write, val);
    }


    public static MVRegOperation reset() {
        return RESET;
    }
}
