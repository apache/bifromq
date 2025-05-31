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

package org.apache.bifromq.basekv.localengine.rocksdb;

import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.bifromq.basekv.localengine.TestUtil;
import org.apache.bifromq.basekv.localengine.metrics.KVSpaceOpMeters;
import io.micrometer.core.instrument.Tags;
import java.io.File;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;
import java.util.function.Predicate;
import lombok.SneakyThrows;
import org.mockito.Mock;
import org.rocksdb.Checkpoint;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

public class KVSpaceCheckpointTest extends AbstractRawRocksDBTest {

    private final String spaceId = "testSpace";
    private ColumnFamilyOptions cfOptions;
    private ColumnFamilyHandle cfHandle;
    private Path cpRootDir;
    private Checkpoint checkpoint;
    private Logger logger;

    @Mock
    private Predicate<String> isLatest;

    @SneakyThrows
    @Override
    protected void doSetup(Method method) {
        super.doSetup(method);
        this.checkpoint = Checkpoint.create(db);
        cpRootDir = Files.createTempDirectory("");
        cfOptions = new ColumnFamilyOptions();
        cfHandle = db.createColumnFamily(new ColumnFamilyDescriptor(spaceId.getBytes(), cfOptions));
        logger = LoggerFactory.getLogger("testLogger");
    }

    @Override
    protected void doTeardown(Method method) {
        super.doTeardown(method);
        cfHandle.close();
        cfOptions.close();
        checkpoint.close();
        TestUtil.deleteDir(cpRootDir.toString());
    }

    @SneakyThrows
    @Test
    public void gc() {
        String cpId = UUID.randomUUID().toString();
        File cpDir = Paths.get(cpRootDir.toAbsolutePath().toString(), cpId).toFile();
        checkpoint.createCheckpoint(cpDir.getAbsolutePath());

        RocksDBKVSpaceCheckpoint spaceSnapshot =
            new RocksDBKVSpaceCheckpoint("testSpace", cpId, cpDir, id -> false,
                new KVSpaceOpMeters("testSpace", Tags.of("tag", "value")), logger);
        spaceSnapshot = null;
        await().forever().until(() -> {
            System.gc();
            return !Files.exists(cpDir.toPath());
        });
    }

    @SneakyThrows
    @Test
    public void gcWithFileKept() {
        String cpId = UUID.randomUUID().toString();
        File cpDir = Paths.get(cpRootDir.toAbsolutePath().toString(), cpId).toFile();
        checkpoint.createCheckpoint(cpDir.getAbsolutePath());
        when(isLatest.test(any())).thenReturn(true);
        RocksDBKVSpaceCheckpoint spaceSnapshot =
            new RocksDBKVSpaceCheckpoint("testSpace", cpId, cpDir, isLatest,
                new KVSpaceOpMeters("testSpace", Tags.of("tag", "value")), logger);
        spaceSnapshot = null;
        await().forever().until(() -> {
            System.gc();
            try {
                verify(isLatest, times(1)).test(cpId);
                return true;
            } catch (Throwable e) {
                return false;
            }
        });
    }
}
