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

package org.apache.bifromq.basekv.localengine.benchmark;

import org.apache.bifromq.basekv.localengine.ICPableKVSpace;
import org.apache.bifromq.basekv.localengine.IKVEngine;
import org.apache.bifromq.basekv.localengine.KVEngineFactory;
import org.apache.bifromq.basekv.localengine.rocksdb.RocksDBCPableKVEngineConfigurator;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.testng.annotations.Test;

@Slf4j
public abstract class BenchmarkTemplate {
    protected IKVEngine<? extends ICPableKVSpace> kvEngine;
    private Path dbRootDir;

    @Setup
    public void setup() {
        try {
            dbRootDir = Files.createTempDirectory("");
        } catch (IOException e) {
            log.error("Failed to create temp dir", e);
        }
        String DB_NAME = "testDB";
        String DB_CHECKPOINT_DIR = "testDB_cp";
        String uuid = UUID.randomUUID().toString();
        RocksDBCPableKVEngineConfigurator configurator = RocksDBCPableKVEngineConfigurator.builder()
            .dbCheckpointRootDir(Paths.get(dbRootDir.toString(), uuid, DB_CHECKPOINT_DIR).toString())
            .dbRootDir(Paths.get(dbRootDir.toString(), uuid, DB_NAME).toString())
            .build();
        kvEngine = KVEngineFactory.createCPable(null, configurator);
        kvEngine.start();
        doSetup();
    }

    @TearDown
    public void tearDown() {
        kvEngine.stop();
        try {
            Files.walk(dbRootDir)
                .sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(File::delete);
            Files.delete(dbRootDir);
        } catch (IOException e) {
            log.error("Failed to delete db root dir", e);
        }
    }

    protected void doSetup() {
    }


    @Test(enabled = false)
    public void test() {
        Options opt = new OptionsBuilder()
            .include(this.getClass().getSimpleName())
            .warmupIterations(5)
            .measurementIterations(10)
            .forks(1)
            .build();
        try {
            new Runner(opt).run();
        } catch (RunnerException e) {
            throw new RuntimeException(e);
        }
    }
}
