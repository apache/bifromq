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

package org.apache.bifromq.basekv.raft.functest.template;

import org.apache.bifromq.basekv.raft.functest.annotation.Cluster;
import org.apache.bifromq.basekv.raft.proto.ClusterConfig;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

@Slf4j
public abstract class RaftGroupTestTemplate {
    private final ClusterConfig defaultClusterConfig =
        ClusterConfig.newBuilder().addVoters("V1").addVoters("V2").addVoters("V3").build();
    private ClusterConfig clusterConfigInUse;

    public void createClusterByAnnotation(Method method) {
        Cluster cluster = method.getAnnotation(Cluster.class);
        clusterConfigInUse = cluster == null ? defaultClusterConfig : build(cluster);
    }

    protected void doSetup(Method method) {
    }

    @BeforeMethod(alwaysRun = true)
    public void setup(Method method) {
        log.info("Test case[{}.{}] start", method.getDeclaringClass().getName(), method.getName());
        createClusterByAnnotation(method);
        doSetup(method);
    }

    @AfterMethod(alwaysRun = true)
    public void teardown(Method method) {
        log.info("Test case[{}.{}] finished, doing teardown",
                method.getDeclaringClass().getName(), method.getName());
        clusterConfigInUse = null;
    }

    public final ClusterConfig clusterConfig() {
        return clusterConfigInUse;
    }

    private ClusterConfig build(Cluster c) {
        return ClusterConfig.newBuilder()
            .addAllVoters(toSet(c.v()))
            .addAllLearners(toSet(c.l()))
            .addAllNextVoters(toSet(c.nv()))
            .addAllNextLearners(toSet(c.nl()))
            .build();
    }

    private Set<String> toSet(String commaSeparatedString) {
        commaSeparatedString = commaSeparatedString.trim();
        Set<String> set = new HashSet<>();
        if (!commaSeparatedString.isEmpty()) {
            set.addAll(Arrays.asList(commaSeparatedString.split(",")));
        }
        return set;
    }
}
