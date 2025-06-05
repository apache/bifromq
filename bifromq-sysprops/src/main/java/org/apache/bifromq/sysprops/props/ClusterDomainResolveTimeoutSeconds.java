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

package org.apache.bifromq.sysprops.props;

import org.apache.bifromq.sysprops.BifroMQSysProp;
import org.apache.bifromq.sysprops.parser.LongParser;

/**
 * The system property used to configure the timeout for resolving the cluster domain.
 */
public class ClusterDomainResolveTimeoutSeconds extends BifroMQSysProp<Long, LongParser> {
    public static final ClusterDomainResolveTimeoutSeconds INSTANCE = new ClusterDomainResolveTimeoutSeconds();

    private ClusterDomainResolveTimeoutSeconds() {
        super("cluster_domain_resolve_timeout_seconds", 120L, LongParser.POSITIVE);
    }
}
