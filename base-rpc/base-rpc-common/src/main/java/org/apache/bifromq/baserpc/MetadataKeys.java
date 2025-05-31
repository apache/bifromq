/*
 * Copyright (c) 2024. The BifroMQ Authors. All Rights Reserved.
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

package org.apache.bifromq.baserpc;

import io.grpc.Metadata;

public class MetadataKeys {
    public static final Metadata.Key<String> TENANT_ID_META_KEY =
        Metadata.Key.of("tenant_id", Metadata.ASCII_STRING_MARSHALLER);
    public static final Metadata.Key<byte[]> CUSTOM_METADATA_META_KEY =
        Metadata.Key.of("custom_metadata-bin", Metadata.BINARY_BYTE_MARSHALLER);
}
