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

package org.apache.bifromq.basekv.raft.exception;

/**
 * Exception thrown during snapshot operations in the Raft protocol.
 * This exception can indicate that a snapshot is obsolete or has other issues.
 */
public class SnapshotException extends RuntimeException {
    private SnapshotException(String message) {
        super(message);
    }

    private SnapshotException(Throwable e) {
        super(e);
    }

    public static ObsoleteSnapshotException obsolete() {
        return new ObsoleteSnapshotException();
    }

    public static NoSnapshotException noSnapshot() {
        return new NoSnapshotException();
    }

    /**
     * Exception indicating that no snapshot is available for installation.
     */
    public static class NoSnapshotException extends SnapshotException {
        private NoSnapshotException() {
            super("No snapshot available");
        }
    }

    /**
     * Exception indicating that the snapshot is obsolete by a newer snapshot during installation.
     */
    public static class ObsoleteSnapshotException extends SnapshotException {
        private ObsoleteSnapshotException() {
            super("The installed snapshot has been obsoleted by a newer snapshot");
        }
    }
}
