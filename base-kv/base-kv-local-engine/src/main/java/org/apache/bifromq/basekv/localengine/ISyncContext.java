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

package org.apache.bifromq.basekv.localengine;

import java.util.function.Supplier;

/**
 * A synchronization context that provides synchronized access to a resource with read-write locks.
 * It supports refreshing the state when needed and performing mutations with proper locking.
 */
public interface ISyncContext {
    /**
     * Get the refresher for read operations.
     *
     * @return the refresher
     */
    IRefresher refresher();

    /**
     * Get the mutator for write operations.
     *
     * @return the mutator
     */
    IMutator mutator();

    /**
     * Callback interface for refreshing state.
     */
    interface IRefresh {
        void refresh(boolean genBumped);
    }

    /**
     * Interface for performing read operations with refresh capability.
     */
    interface IRefresher {
        void runIfNeeded(IRefresh refresh);

        <T> T call(Supplier<T> supplier);
    }

    /**
     * Callback interface for performing mutations.
     */
    interface IMutation {
        /**
         * Perform mutation, return true if generation is bumped.
         *
         * @return true if generation is bumped
         */
        boolean mutate();
    }

    /**
     * Interface for performing mutations with write locking.
     */
    interface IMutator {
        /**
         * Perform mutation with write lock.
         *
         * @param mutation the mutation to perform
         * @return boolean indicating if generation is bumped after the mutation
         */
        boolean run(IMutation mutation);
    }
}
