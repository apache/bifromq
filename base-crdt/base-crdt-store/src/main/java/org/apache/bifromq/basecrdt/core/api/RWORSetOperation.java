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

package org.apache.bifromq.basecrdt.core.api;

import com.google.protobuf.ByteString;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.ToString;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
@ToString
public final class RWORSetOperation implements ICRDTOperation {
    private static final RWORSetOperation CLEAR = new RWORSetOperation(Type.Clear, ByteString.EMPTY);


    public enum Type {
        Add, Remove, Clear
    }

    public final Type type;
    public final ByteString element;

    public static RWORSetOperation add(ByteString e) {
        return new RWORSetOperation(Type.Add, e);
    }

    public static RWORSetOperation remove(ByteString e) {
        return new RWORSetOperation(Type.Remove, e);
    }

    public static RWORSetOperation clear() {
        return CLEAR;
    }
}
