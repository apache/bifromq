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
import com.google.protobuf.UnsafeByteOperations;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TestUtil {
    public static long toLong(ByteString b) {
        assert b.size() == Long.BYTES;
        ByteBuffer buffer = b.asReadOnlyByteBuffer();
        return buffer.getLong();
    }

    public static ByteString toByteString(long l) {
        return UnsafeByteOperations.unsafeWrap(toBytes(l));
    }

    public static byte[] toBytes(long l) {
        return ByteBuffer.allocate(Long.BYTES).putLong(l).array();
    }

    public static ByteString toByteStringNativeOrder(long l) {
        return UnsafeByteOperations.unsafeWrap(ByteBuffer.allocate(Long.BYTES)
            .order(ByteOrder.nativeOrder())
            .putLong(l).array());
    }


    public static long toLongNativeOrder(ByteString b) {
        assert b.size() == Long.BYTES;
        ByteBuffer buffer = b.asReadOnlyByteBuffer().order(ByteOrder.nativeOrder());
        return buffer.getLong();
    }

    @SuppressWarnings("unchecked")
    public static <T> T getField(Object targetObject, String fieldName) {
        try {
            Field field = targetObject.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            return (T) field.get(targetObject);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            log.warn("get field {} from {} failed: {}", targetObject, fieldName, e.getMessage());
        }
        return null;
    }

    public static void deleteDir(String dir) {
        try {
            Files.walk(Paths.get(dir))
                .sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(File::delete);
        } catch (IOException e) {
            log.error("Failed to delete db root dir", e);
        }
    }
}
