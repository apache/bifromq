/*
 * Copyright (c) 2023. Baidu, Inc. All Rights Reserved.
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

package io.grpc.netty;

import static com.google.common.base.Preconditions.checkNotNull;

import java.net.SocketAddress;

/**
 * Custom SocketAddress class for {@link InProcTransport}.
 */
public final class InProcSocketAddress extends SocketAddress {
    private final String name;

    /**
     * Construct an address for a server identified by name.
     *
     * @param name The name of the inprocess server.
     * @since 1.0.0
     */
    public InProcSocketAddress(String name) {
        this.name = checkNotNull(name, "name");
    }

    /**
     * Gets the name of the inprocess server.
     *
     * @since 1.0.0
     */
    public String getName() {
        return name;
    }

    /**
     * Returns {@link #getName}.
     *
     * @since 1.14.0
     */
    @Override
    public String toString() {
        return name;
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    /**
     * Returns {@code true} if the object is of the same type and server names match.
     *
     * @since 1.15.0
     */
    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof InProcSocketAddress)) {
            return false;
        }
        return name.equals(((InProcSocketAddress) obj).name);
    }
}
