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

import org.apache.bifromq.basecrdt.core.api.IMVReg;
import org.apache.bifromq.basecrdt.core.api.MVRegOperation;
import org.apache.bifromq.basecrdt.proto.Replica;
import com.google.protobuf.ByteString;
import java.util.Iterator;

class MVReg extends CausalCRDT<IDotFunc, MVRegOperation> implements IMVReg {
    MVReg(Replica replica, DotStoreAccessor<IDotFunc> dotStoreAccessor,
          CRDTOperationExecutor<MVRegOperation> executor) {
        super(replica, dotStoreAccessor, executor);
    }

    @Override
    public Iterator<ByteString> read() {
        return dotStoreAccessor.fetch().values().iterator();
    }
}
