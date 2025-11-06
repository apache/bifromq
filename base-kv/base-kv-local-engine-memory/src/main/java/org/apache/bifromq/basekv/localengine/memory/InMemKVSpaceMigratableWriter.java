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

package org.apache.bifromq.basekv.localengine.memory;

import java.util.function.Consumer;
import org.apache.bifromq.basekv.localengine.IKVSpaceIterator;
import org.apache.bifromq.basekv.localengine.IKVSpaceMigratableWriter;
import org.apache.bifromq.basekv.localengine.IRestoreSession;
import org.apache.bifromq.basekv.localengine.ISyncContext;
import org.apache.bifromq.basekv.localengine.KVEngineException;
import org.apache.bifromq.basekv.localengine.metrics.KVSpaceOpMeters;
import org.apache.bifromq.basekv.proto.Boundary;
import org.slf4j.Logger;

public class InMemKVSpaceMigratableWriter<E extends InMemKVEngine<E, T>, T extends InMemKVSpace<E, T>>
    extends InMemKVSpaceWriter<E, T> implements IKVSpaceMigratableWriter {

    InMemKVSpaceMigratableWriter(String id,
                                 InMemKVSpaceEpoch epoch,
                                 E engine,
                                 ISyncContext syncContext,
                                 Consumer<Boolean> afterWrite,
                                 Consumer<InMemKVSpaceWriterHelper.WriteImpact> impactListener,
                                 KVSpaceOpMeters readOpMeters,
                                 Logger logger) {
        super(id, epoch, engine, syncContext, afterWrite, impactListener, readOpMeters, logger);
    }

    @Override
    public IRestoreSession migrateTo(String targetSpaceId, Boundary boundary) {
        try {
            InMemCPableKVSpace targetKVSpace = (InMemCPableKVSpace) engine.createIfMissing(targetSpaceId);
            IRestoreSession session = targetKVSpace.startRestore(((count, bytes) ->
                logger.debug("Migrate {} kv to space[{}] from space[{}]: startKey={}, endKey={}",
                    count, targetSpaceId, id, boundary.getStartKey().toStringUtf8(),
                    boundary.getEndKey().toStringUtf8())));
            // move data
            try (IKVSpaceIterator itr = new InMemKVSpaceIterator(epoch.dataMap(), boundary)) {
                for (itr.seekToFirst(); itr.isValid(); itr.next()) {
                    session.put(itr.key(), itr.value());
                }
            }
            // clear moved data in left range
            helper.clear(id, boundary);
            return session;
        } catch (Throwable e) {
            throw new KVEngineException("Delete range in batch failed", e);
        }
    }
}
