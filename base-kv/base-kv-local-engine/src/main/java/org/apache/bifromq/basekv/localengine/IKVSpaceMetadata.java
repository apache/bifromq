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

package org.apache.bifromq.basekv.localengine;

import org.apache.bifromq.basekv.proto.Boundary;
import com.google.protobuf.ByteString;
import java.util.Optional;

/**
 * Interface for accessing space metadata.
 */
public interface IKVSpaceMetadata {
    /**
     * Get the id of the space.
     *
     * @return the id of the space
     */
    String id();

    /**
     * Get the metadata in key-value pair.
     *
     * @param metaKey the key of the metadata
     * @return the value of the metadata
     */
    Optional<ByteString> metadata(ByteString metaKey);

    /**
     * Get the size of the space.
     *
     * @return the size of the space
     */
    long size();

    /**
     * Get the size of the space in the specified boundary.
     *
     * @param boundary the boundary
     * @return the size of the space in the specified boundary
     */
    long size(Boundary boundary);
}
