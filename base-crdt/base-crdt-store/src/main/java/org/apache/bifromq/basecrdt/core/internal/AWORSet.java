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

package org.apache.bifromq.basecrdt.core.internal;

import org.apache.bifromq.basecrdt.core.api.AWORSetOperation;
import org.apache.bifromq.basecrdt.core.api.IAWORSet;
import org.apache.bifromq.basecrdt.proto.Replica;
import com.google.protobuf.ByteString;
import java.util.Iterator;

class AWORSet extends CausalCRDT<IDotMap, AWORSetOperation> implements IAWORSet {

    AWORSet(Replica replica, DotStoreAccessor<IDotMap> dotStoreAccessor,
            CRDTOperationExecutor<AWORSetOperation> executor) {
        super(replica, dotStoreAccessor, executor);
    }

    @Override
    public boolean contains(ByteString element) {
        return dotStoreAccessor.fetch().subDotSet(element).isPresent();
    }

    @Override
    public Iterator<ByteString> elements() {
        return dotStoreAccessor.fetch().dotSetKeys();
    }
}
