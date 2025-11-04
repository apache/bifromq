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

package org.apache.bifromq.basekv.localengine.rocksdb;

import static com.google.protobuf.UnsafeByteOperations.unsafeWrap;
import static java.util.Collections.singletonList;
import static org.apache.bifromq.basekv.localengine.rocksdb.Keys.DATA_SECTION_END;
import static org.apache.bifromq.basekv.localengine.rocksdb.Keys.DATA_SECTION_START;
import static org.apache.bifromq.basekv.localengine.rocksdb.Keys.META_SECTION_END;
import static org.apache.bifromq.basekv.localengine.rocksdb.Keys.META_SECTION_START;
import static org.apache.bifromq.basekv.localengine.rocksdb.Keys.fromMetaKey;
import static org.apache.bifromq.basekv.localengine.rocksdb.Keys.toDataKey;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.compare;
import static org.rocksdb.SizeApproximationFlag.INCLUDE_FILES;
import static org.rocksdb.SizeApproximationFlag.INCLUDE_MEMTABLES;

import com.google.protobuf.ByteString;
import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.bifromq.basekv.localengine.KVEngineException;
import org.apache.bifromq.basekv.proto.Boundary;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.DBOptions;
import org.rocksdb.Range;
import org.rocksdb.RocksDB;
import org.rocksdb.Slice;

class RocksDBHelper {
    static RocksDBHandle openDBInDir(File dir, DBOptions dbOptions, ColumnFamilyDescriptor cfDesc) {
        try {
            List<ColumnFamilyHandle> cfHandles = new ArrayList<>();
            RocksDB db = RocksDB.open(dbOptions, dir.getAbsolutePath(), Collections.singletonList(cfDesc), cfHandles);
            assert cfHandles.size() == 1;
            ColumnFamilyHandle cf = cfHandles.get(0);
            return new RocksDBHandle(db, cf);
        } catch (Throwable e) {
            throw new KVEngineException("Open RocksDB at dir failed", e);
        }
    }

    static void deleteDir(Path path) throws IOException {
        if (!path.toFile().exists()) {
            return;
        }
        Files.walkFileTree(path, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                try {
                    Files.delete(file);
                } catch (IOException e) {
                    // do nothing
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) {
                try {
                    Files.delete(dir);
                } catch (IOException e) {
                    // do nothing
                }
                return FileVisitResult.CONTINUE;
            }
        });
    }

    static long sizeOfBoundary(IRocksDBKVSpaceEpoch dbHandle, Boundary boundary) {
        byte[] start =
            !boundary.hasStartKey() ? DATA_SECTION_START : toDataKey(boundary.getStartKey().toByteArray());
        byte[] end =
            !boundary.hasEndKey() ? DATA_SECTION_END : toDataKey(boundary.getEndKey().toByteArray());
        if (compare(start, end) < 0) {
            try (Slice startSlice = new Slice(start); Slice endSlice = new Slice(end)) {
                Range range = new Range(startSlice, endSlice);
                return dbHandle.db()
                    .getApproximateSizes(dbHandle.cf(), singletonList(range), INCLUDE_MEMTABLES, INCLUDE_FILES)[0];
            }
        }
        return 0;
    }

    static Map<ByteString, ByteString> getMetadata(IRocksDBKVSpaceEpoch dbHandle) {
        try (RocksDBKVEngineIterator metaItr = new RocksDBKVEngineIterator(dbHandle.db(), dbHandle.cf(),
            null,
            META_SECTION_START,
            META_SECTION_END)) {
            Map<ByteString, ByteString> metaMap = new HashMap<>();
            for (metaItr.seekToFirst(); metaItr.isValid(); metaItr.next()) {
                metaMap.put(fromMetaKey(metaItr.key()), unsafeWrap(metaItr.value()));
            }
            return metaMap;
        }
    }

    record RocksDBHandle(RocksDB db, ColumnFamilyHandle cf) {
    }
}
