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
import java.util.Iterator;

public interface IORMap extends ICausalCRDT<ORMapOperation> {
    interface ORMapKey {
        ByteString key();

        CausalCRDTType valueType();
    }

    @Override
    default CausalCRDTType type() {
        return CausalCRDTType.ormap;
    }

    /**
     * Non-bottom keys
     *
     * @return iterator of keys
     */
    Iterator<ORMapKey> keys();

    IAWORSet getAWORSet(ByteString... keys);

    IRWORSet getRWORSet(ByteString... keys);

    ICCounter getCCounter(ByteString... keys);

    IMVReg getMVReg(ByteString... keys);

    IDWFlag getDWFlag(ByteString... keys);

    IEWFlag getEWFlag(ByteString... keys);

    IORMap getORMap(ByteString... keys);
}
