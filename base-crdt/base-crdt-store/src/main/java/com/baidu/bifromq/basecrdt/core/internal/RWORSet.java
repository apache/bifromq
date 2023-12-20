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

package com.baidu.bifromq.basecrdt.core.internal;

import com.baidu.bifromq.basecrdt.core.api.IRWORSet;
import com.baidu.bifromq.basecrdt.core.api.RWORSetOperation;
import com.baidu.bifromq.basecrdt.proto.Replica;
import com.google.common.collect.AbstractIterator;
import com.google.protobuf.ByteString;
import java.util.Iterator;

class RWORSet extends CausalCRDT<IDotMap, RWORSetOperation> implements IRWORSet {
    public static final ByteString TRUE = ByteString.copyFrom(new byte[] {(byte) 0xFF});
    public static final ByteString FALSE = ByteString.copyFrom(new byte[] {(byte) 0x00});


    RWORSet(Replica replica, DotStoreAccessor<IDotMap> dotStoreAccessor,
            CRDTOperationExecutor<RWORSetOperation> executor) {
        super(replica, dotStoreAccessor, executor);
    }

    @Override
    public boolean contains(ByteString element) {
        IDotMap dotStore = dotStoreAccessor.fetch();
        IDotMap valueDotMap = dotStore.subDotMap(element).orElse(DotMap.BOTTOM);
        if (valueDotMap.subDotSet(FALSE).orElse(DotSet.BOTTOM).isBottom()) {
            return !valueDotMap.subDotSet(TRUE).orElse(DotSet.BOTTOM).isBottom();
        }
        return false;
    }

    @Override
    public Iterator<ByteString> elements() {
        return new AbstractIterator<>() {
            private final Iterator<ByteString> dotMapKeys = dotStoreAccessor.fetch().dotMapKeys();

            @Override
            protected ByteString computeNext() {
                if (dotMapKeys.hasNext()) {
                    ByteString element = dotMapKeys.next();
                    if (contains(element)) {
                        return element;
                    } else {
                        return computeNext();
                    }
                }
                return endOfData();
            }
        };
    }
}
