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

import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import lombok.SneakyThrows;
import org.apache.bifromq.basekv.localengine.MockableTest;
import org.apache.bifromq.basekv.localengine.TestUtil;
import org.mockito.Mockito;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;

abstract class AbstractRawRocksDBTest extends MockableTest {
    static {
        RocksDB.loadLibrary();
    }

    protected RocksDB db;
    protected ColumnFamilyHandle cfHandle;
    private Path dbRootDir;
    private Options options;

    @SneakyThrows
    @Override
    protected void doSetup(Method method) {
        super.doSetup(method);
        dbRootDir = Files.createTempDirectory("");
        options = new Options().setCreateIfMissing(true);
        db = Mockito.spy(RocksDB.open(options, dbRootDir.toAbsolutePath().toString()));
        cfHandle = db.getDefaultColumnFamily();
    }

    @Override
    protected void doTeardown(Method method) {
        super.doTeardown(method);
        cfHandle.close();
        db.close();
        options.close();
        TestUtil.deleteDir(dbRootDir.toString());
    }
}
