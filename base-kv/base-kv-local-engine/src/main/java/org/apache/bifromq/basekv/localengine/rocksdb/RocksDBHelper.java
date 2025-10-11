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

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.bifromq.basekv.localengine.KVEngineException;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;

public class RocksDBHelper {
    public static RocksDBHandle openDBInDir(File dir, DBOptions dbOptions, ColumnFamilyDescriptor cfDesc) {
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

    public static void deleteDir(Path path) throws IOException {
        Files.walkFileTree(path, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    public record RocksDBHandle(RocksDB db, ColumnFamilyHandle cf) {
    }
}
