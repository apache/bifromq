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

import static org.apache.bifromq.basekv.localengine.StructUtil.toValue;
import static org.apache.bifromq.basekv.localengine.rocksdb.RocksDBDefaultConfigs.COMPACT_MIN_TOMBSTONE_KEYS;
import static org.apache.bifromq.basekv.localengine.rocksdb.RocksDBDefaultConfigs.DB_CHECKPOINT_ROOT_DIR;
import static org.apache.bifromq.basekv.localengine.rocksdb.RocksDBDefaultConfigs.DB_ROOT_DIR;
import static org.apache.bifromq.basekv.localengine.rocksdb.RocksDBHelper.deleteDir;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.google.protobuf.ByteString;
import com.google.protobuf.Struct;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig;
import io.micrometer.core.instrument.distribution.pause.PauseDetector;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.bifromq.basekv.localengine.ICPableKVSpace;
import org.apache.bifromq.basekv.localengine.IKVSpace;
import org.apache.bifromq.basekv.localengine.IKVSpaceReader;
import org.apache.bifromq.basekv.localengine.IKVSpaceWriter;
import org.apache.bifromq.basekv.localengine.IWALableKVSpace;
import org.apache.bifromq.basekv.localengine.KVEngineException;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class RocksDBExecutorLifecycleTest {
    private static final String SPACE_ID = "executor-lifecycle";
    private static final ByteString KEY = ByteString.copyFromUtf8("kept-key");
    private static final ByteString VALUE = ByteString.copyFromUtf8("kept-value");

    @DataProvider
    public Object[][] scenarios() {
        return new Object[][] {
            {false, "close"}, {false, "destroy"}, {false, "stop"},
            {true, "close"}, {true, "destroy"}, {true, "stop"},
            {false, "compactor"}, {true, "compactor"}, {true, "flusher"},
            {false, "constructor-failure"}, {false, "open-failure"}, {true, "open-failure"}
        };
    }

    @Test(dataProvider = "scenarios")
    public void executorLifecycle(boolean wal, String scenario) throws Exception {
        Path root = Files.createTempDirectory("rocksdb-executor-");
        Path output = root.resolve("child.log");
        Process child = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
            "-ea", "-cp", System.getProperty("java.class.path"), RocksDBExecutorLifecycleTest.class.getName(),
            Boolean.toString(wal), scenario, root.toString())
            .redirectErrorStream(true).redirectOutput(output.toFile()).start();
        try {
            assertTrue(child.waitFor(45, TimeUnit.SECONDS), "Child did not exit: " + Files.readString(output));
            assertEquals(child.exitValue(), 0, Files.readString(output));
        } finally {
            if (child.isAlive()) {
                child.destroyForcibly();
                assertTrue(child.waitFor(10, TimeUnit.SECONDS));
            }
            deleteDir(root);
        }
    }

    public static void main(String[] args) {
        try {
            boolean wal = Boolean.parseBoolean(args[0]);
            String scenario = args[1];
            Path root = Path.of(args[2]);
            if (scenario.endsWith("failure")) {
                failedCreation(wal, scenario, root);
            } else if (scenario.equals("compactor") || scenario.equals("flusher")) {
                closeWaitsForTask(wal, scenario, root);
            } else {
                lifecycle(wal, scenario, root);
            }
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(1);
        }
        // A successful child must exit without System.exit, even after compaction starts a worker.
    }

    private static void lifecycle(boolean wal, String action, Path root) throws Exception {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        try {
            Metrics.addRegistry(registry);
            RocksDBKVEngine<?> engine = newEngine(wal, root);
            engine.start();
            IKVSpace space = engine.createIfMissing(SPACE_ID);
            IKVSpace other = engine.createIfMissing("other");
            List<String> names = wal ? List.of("compactor", "flusher") : List.of("compactor");
            List<Timer> oldTimers = names.stream().map(name -> registry.get("kvspace.executor")
                .tags("spaceId", SPACE_ID, "name", name).timer()).toList();
            List<Gauge> oldGauges = names.stream().map(name -> registry.get("kvspace.executor.pool.size")
                .tags("spaceId", SPACE_ID, "name", name).gauge()).toList();
            exercise(space, registry);
            exercise(other, registry);
            List<Thread> workers = workers(SPACE_ID);
            assertEquals(workers.size(), wal ? 2 : 1);
            assertEquals(executorMeters(registry, SPACE_ID), wal ? 18L : 9L);
            if (action.equals("stop")) {
                engine.stop();
            } else {
                if (action.equals("destroy")) {
                    space.destroy();
                } else {
                    space.close();
                }
                assertEquals(executorMeters(registry, "other"), wal ? 18L : 9L);
                try (IKVSpaceReader reader = other.reader()) {
                    assertEquals(reader.get(KEY).orElseThrow(), VALUE);
                }
            }
            assertEquals(executorMeters(registry, SPACE_ID), 0L);
            assertStopped(workers);
            if (!action.equals("stop")) {
                engine.stop();
            }
            assertEquals(executorMeters(registry, "other"), 0L);

            engine = newEngine(wal, root);
            engine.start();
            IKVSpace replacement = engine.createIfMissing(SPACE_ID);
            for (int i = 0; i < names.size(); i++) {
                Timer newTimer = registry.get("kvspace.executor").tags("spaceId", SPACE_ID, "name", names.get(i)).timer();
                Gauge newGauge = registry.get("kvspace.executor.pool.size")
                    .tags("spaceId", SPACE_ID, "name", names.get(i)).gauge();
                assertNotSame(newTimer, oldTimers.get(i));
                assertNotSame(newGauge, oldGauges.get(i));
                assertEquals(newTimer.count(), 0L);
                assertEquals(newGauge.value(), 0.0);
            }
            try (IKVSpaceReader reader = replacement.reader()) {
                assertEquals(reader.get(KEY).isPresent(), !action.equals("destroy"));
                if (!action.equals("destroy")) {
                    assertEquals(reader.get(KEY).orElseThrow(), VALUE);
                }
            }
            exercise(replacement, registry);
            for (String name : names) {
                assertEquals(registry.get("kvspace.executor.pool.size")
                    .tags("spaceId", SPACE_ID, "name", name).gauge().value(), 1.0);
            }
            List<Thread> replacementWorkers = workers(SPACE_ID);
            engine.stop();
            assertStopped(replacementWorkers);
            assertEquals(executorMeters(registry, SPACE_ID), 0L);
        } finally {
            Metrics.removeRegistry(registry);
            registry.close();
        }
    }

    private static void closeWaitsForTask(boolean wal, String worker, Path root) throws Exception {
        BlockingRegistry registry = new BlockingRegistry(worker);
        try {
            Metrics.addRegistry(registry);
            RocksDBKVEngine<?> engine = newEngine(wal, root);
            engine.start();
            IKVSpace space = engine.createIfMissing(SPACE_ID);
            triggerCompaction(space);
            if (worker.equals("flusher")) {
                ((IWALableKVSpace) space).flush().get(10, TimeUnit.SECONDS);
            }
            assertTrue(registry.entered.await(10, TimeUnit.SECONDS));
            Path spaceDir = root.resolve("data").resolve(SPACE_ID);
            Path dbDir = wal ? spaceDir : spaceDir.resolve(
                Files.readString(spaceDir.resolve(RocksDBCPableKVSpace.ACTIVE_GEN_POINTER)).trim());
            CompletableFuture<Void> closed = CompletableFuture.runAsync(space::close);
            try {
                assertThrows(TimeoutException.class, () -> closed.get(300, TimeUnit.MILLISECONDS));
                try (Options options = new Options()) {
                    RocksDBException locked = org.testng.Assert.expectThrows(RocksDBException.class,
                        () -> RocksDB.open(options, dbDir.toString()));
                    assertTrue(locked.getMessage().toLowerCase().contains("lock"), locked.getMessage());
                }
            } finally {
                registry.release.countDown();
            }
            closed.get(10, TimeUnit.SECONDS);
            try (Options options = new Options(); RocksDB db = RocksDB.open(options, dbDir.toString())) {
                assertTrue(db.isOwningHandle());
            }
            engine.stop();
        } finally {
            Metrics.removeRegistry(registry);
            registry.close();
        }
    }

    private static void failedCreation(boolean wal, String scenario, Path root) throws Exception {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        try {
            Metrics.addRegistry(registry);
            RocksDBKVEngine<?> engine = newEngine(wal, root);
            engine.start();
            if (scenario.equals("constructor-failure")) {
                Path blocked = root.resolve("checkpoints").resolve(SPACE_ID);
                Files.createFile(blocked);
                assertThrows(Exception.class, () -> engine.createIfMissing(SPACE_ID));
                assertEquals(executorMeters(registry, SPACE_ID), 0L);
                Files.delete(blocked);
            } else {
                Path spaceDir = root.resolve("data").resolve(SPACE_ID);
                Path dbDir = wal ? spaceDir : spaceDir.resolve("generation");
                Files.createDirectories(dbDir);
                if (!wal) {
                    Files.writeString(spaceDir.resolve(RocksDBCPableKVSpace.ACTIVE_GEN_POINTER), "generation");
                }
                try (Options options = new Options().setCreateIfMissing(true);
                     RocksDB locked = RocksDB.open(options, dbDir.toString())) {
                    assertThrows(KVEngineException.class, () -> engine.createIfMissing(SPACE_ID));
                    assertEquals(executorMeters(registry, SPACE_ID), 0L);
                }
            }
            IKVSpace retried = engine.createIfMissing(SPACE_ID);
            exercise(retried, registry);
            engine.stop();
            assertEquals(executorMeters(registry, SPACE_ID), 0L);
        } finally {
            Metrics.removeRegistry(registry);
            registry.close();
        }
    }

    private static RocksDBKVEngine<?> newEngine(boolean wal, Path root) {
        Struct.Builder config = (wal ? RocksDBDefaultConfigs.WAL : RocksDBDefaultConfigs.CP).toBuilder()
            .putFields(DB_ROOT_DIR, toValue(root.resolve("data").toString()))
            .putFields(COMPACT_MIN_TOMBSTONE_KEYS, toValue(1));
        if (!wal) {
            config.putFields(DB_CHECKPOINT_ROOT_DIR, toValue(root.resolve("checkpoints").toString()));
        }
        return wal ? new RocksDBWALableKVEngine(null, config.build())
            : new RocksDBCPableKVEngine(null, config.build());
    }

    private static void triggerCompaction(IKVSpace space) {
        IKVSpaceWriter writer = space instanceof ICPableKVSpace cp ? cp.toWriter()
            : ((IWALableKVSpace) space).toWriter();
        writer.put(KEY, VALUE)
            .delete(ByteString.copyFromUtf8("deleted-1"))
            .delete(ByteString.copyFromUtf8("deleted-2")).done();
    }

    private static void exercise(IKVSpace space, SimpleMeterRegistry registry) throws Exception {
        triggerCompaction(space);
        await().atMost(Duration.ofSeconds(10)).until(() -> registry.get("kvspace.executor")
            .tags("spaceId", space.id(), "name", "compactor").timer().count() > 0);
        if (space instanceof IWALableKVSpace wal) {
            wal.flush().get(10, TimeUnit.SECONDS);
            await().atMost(Duration.ofSeconds(10)).until(() -> registry.get("kvspace.executor")
                .tags("spaceId", space.id(), "name", "flusher").timer().count() > 0);
        }
    }

    private static long executorMeters(SimpleMeterRegistry registry, String spaceId) {
        return registry.getMeters().stream().map(Meter::getId)
            .filter(id -> id.getName().startsWith("kvspace.executor") && spaceId.equals(id.getTag("spaceId"))).count();
    }

    private static List<Thread> workers(String spaceId) {
        return Thread.getAllStackTraces().keySet().stream().filter(thread -> thread.isAlive()
            && (thread.getName().startsWith("kvspace-compactor-" + spaceId)
            || thread.getName().startsWith("kvspace-flusher-" + spaceId))).toList();
    }

    private static void assertStopped(List<Thread> workers) throws InterruptedException {
        for (Thread worker : workers) {
            worker.join(10000);
            assertFalse(worker.isAlive(), worker.getName());
        }
    }

    private static class BlockingRegistry extends SimpleMeterRegistry {
        private final String worker;
        private final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);

        BlockingRegistry(String worker) {
            this.worker = worker;
        }

        @Override
        protected Timer newTimer(Meter.Id id, DistributionStatisticConfig config, PauseDetector pauseDetector) {
            Timer timer = super.newTimer(id, config, pauseDetector);
            if (id.getName().equals("kvspace.executor") && worker.equals(id.getTag("name"))) {
                Timer monitored = spy(timer);
                doAnswer(call -> {
                    entered.countDown();
                    assertTrue(release.await(15, TimeUnit.SECONDS));
                    return call.callRealMethod();
                }).when(monitored).record(anyLong(), any(TimeUnit.class));
                return monitored;
            }
            return timer;
        }
    }
}
