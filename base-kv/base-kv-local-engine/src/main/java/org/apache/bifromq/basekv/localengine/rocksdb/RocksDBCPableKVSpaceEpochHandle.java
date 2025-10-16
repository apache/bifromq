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

import io.micrometer.core.instrument.Tags;
import java.io.File;
import java.lang.ref.Cleaner;
import java.util.function.Predicate;
import org.apache.bifromq.baseenv.EnvProvider;
import org.slf4j.Logger;

class RocksDBCPableKVSpaceEpochHandle extends RocksDBKVSpaceEpochHandle<RocksDBCPableKVEngineConfigurator> {
    private static final Cleaner CLEANER = Cleaner.create(
        EnvProvider.INSTANCE.newThreadFactory("kvspace-epoch-cleaner", true));
    private final SpaceMetrics metrics;
    private final Cleaner.Cleanable cleanable;

    RocksDBCPableKVSpaceEpochHandle(String id,
                                    File dir,
                                    RocksDBCPableKVEngineConfigurator configurator,
                                    Predicate<String> isRetired,
                                    Logger logger,
                                    Tags tags) {
        super(dir, configurator, logger);
        this.metrics = new SpaceMetrics(id, db, cf, cfDesc.getOptions(), tags.and("gen", dir.getName()), logger);
        cleanable = CLEANER.register(this, new ClosableResources(id, dir.getName(), dbOptions, cfDesc, cf, db,
            checkpoint, dir, isRetired, metrics, logger));
    }

    @Override
    public void close() {
        cleanable.clean();
    }
}
