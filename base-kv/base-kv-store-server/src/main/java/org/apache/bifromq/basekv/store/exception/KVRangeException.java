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

package org.apache.bifromq.basekv.store.exception;

import org.apache.bifromq.basekv.proto.KVRangeDescriptor;

public class KVRangeException extends RuntimeException {
    public KVRangeException(String message) {
        super(message);
    }

    public KVRangeException(String message, Throwable cause) {
        super(message, cause);
    }

    public static class TryLater extends KVRangeException {

        public TryLater(String message) {
            super(message);
        }

        public TryLater(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public static class BadVersion extends KVRangeException {
        public final KVRangeDescriptor latest;

        public BadVersion(String message) {
            this(message, null);
        }

        public BadVersion(String message, KVRangeDescriptor latest) {
            super(message);
            this.latest = latest;
        }
    }

    public static class BadRequest extends KVRangeException {

        public BadRequest(String message) {
            super(message);
        }

        public BadRequest(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public static class InternalException extends KVRangeException {

        public InternalException(String message) {
            super(message);
        }

        public InternalException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
