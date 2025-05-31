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

package org.apache.bifromq.basekv.localengine;

import com.google.protobuf.ByteString;

/**
 * Interface for updating space metadata.
 *
 * @param <T> the type of the updater
 */
public interface IKVSpaceMetadataUpdatable<T extends IKVSpaceMetadataUpdatable<T>> extends IKVSpaceMetadata {
    /**
     * Update metadata in key-value pair.
     *
     * @param metaKey   the key of the metadata
     * @param metaValue the value of the metadata
     */
    T metadata(ByteString metaKey, ByteString metaValue);
}
