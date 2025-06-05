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
import lombok.ToString;

@ToString
public abstract class ORMapOperation implements ICRDTOperation {
    public interface ORMapUpdater {
        ORMapUpdate<AWORSetOperation> with(AWORSetOperation op);

        ORMapUpdate<RWORSetOperation> with(RWORSetOperation op);

        ORMapUpdate<DWFlagOperation> with(DWFlagOperation op);

        ORMapUpdate<EWFlagOperation> with(EWFlagOperation op);

        ORMapUpdate<MVRegOperation> with(MVRegOperation op);

        ORMapUpdate<CCounterOperation> with(CCounterOperation op);

        ORMapUpdate<ORMapOperation> with(ORMapOperation op);
    }

    public interface ORMapRemover {
        ORMapRemove of(CausalCRDTType valueType);
    }

    public enum Type {
        UpdateKey,
        RemoveKey,
        Clear
    }

    public final Type type;
    public final ByteString[] keyPath;

    ORMapOperation(Type type, ByteString[] keyPath) {
        this.type = type;
        this.keyPath = keyPath;
    }

    public static ORMapRemover remove(ByteString... keyPath) {
        return valueType -> new ORMapRemove(Type.RemoveKey, keyPath, valueType);
    }

    public static ORMapUpdater update(ByteString... keyPath) {
        return new ORMapUpdater() {
            @Override
            public ORMapUpdate<AWORSetOperation> with(AWORSetOperation op) {
                return new ORMapUpdate<>(Type.UpdateKey, keyPath, op);
            }

            @Override
            public ORMapUpdate<RWORSetOperation> with(RWORSetOperation op) {
                return new ORMapUpdate<>(Type.UpdateKey, keyPath, op);
            }

            @Override
            public ORMapUpdate<DWFlagOperation> with(DWFlagOperation op) {
                return new ORMapUpdate<>(Type.UpdateKey, keyPath, op);
            }

            @Override
            public ORMapUpdate<EWFlagOperation> with(EWFlagOperation op) {
                return new ORMapUpdate<>(Type.UpdateKey, keyPath, op);
            }

            @Override
            public ORMapUpdate<MVRegOperation> with(MVRegOperation op) {
                return new ORMapUpdate<>(Type.UpdateKey, keyPath, op);
            }

            @Override
            public ORMapUpdate<CCounterOperation> with(CCounterOperation op) {
                return new ORMapUpdate<>(Type.UpdateKey, keyPath, op);
            }

            @Override
            public ORMapUpdate<ORMapOperation> with(ORMapOperation op) {
                return new ORMapUpdate<>(Type.UpdateKey, keyPath, op);
            }

        };
    }

    public static class ORMapUpdate<T extends ICRDTOperation> extends ORMapOperation {
        public final T valueOp;

        private ORMapUpdate(Type type, ByteString[] keyPath, T valueOp) {
            super(type, keyPath);
            this.valueOp = valueOp;
        }
    }

    public static class ORMapRemove extends ORMapOperation {
        public CausalCRDTType valueType;

        private ORMapRemove(Type type, ByteString[] keyPath, CausalCRDTType valueType) {
            super(type, keyPath);
            this.valueType = valueType;
        }
    }
}
