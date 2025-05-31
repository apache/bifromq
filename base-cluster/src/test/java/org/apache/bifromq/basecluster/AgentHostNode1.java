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

package org.apache.bifromq.basecluster;

import org.apache.bifromq.basecluster.memberlist.agent.IAgent;
import org.apache.bifromq.basecluster.memberlist.agent.IAgentMember;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.logging.LoggingMeterRegistry;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AgentHostNode1 {

    public static void main(String[] args) {
        Metrics.addRegistry(new LoggingMeterRegistry());

        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        AgentHostOptions opt = AgentHostOptions.builder()
            .autoHealingTimeout(Duration.ofSeconds(300))
            .addr("127.0.0.1")
            .port(3334)
            .build();
        IAgentHost host = IAgentHost.newInstance(opt);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            MoreExecutors.shutdownAndAwaitTermination(executorService, 5, TimeUnit.SECONDS);
            host.close();
        }));
        IAgent agent = host.host("service1");
        IAgentMember agentMember = agent.register("AgentNode1");
        agentMember.metadata(ByteString.copyFromUtf8("Greeting"));
        agent.membership().subscribe(agentNodes -> log.info("Agent[service1] members:\n{}", agentNodes));
        host.membership().subscribe(memberList -> log.info("AgentHosts:\n{}", memberList));
        agentMember.receive()
            .subscribe(msg -> log.info("AgentMessage: {}", msg));
        executorService.scheduleAtFixedRate(() -> {
            agentMember.broadcast(ByteString.copyFromUtf8("hello:" + ThreadLocalRandom.current().nextInt()), true)
                .whenComplete((v, e) -> {
                    if (e != null) {
                        log.error("failed to broadcast", e);
                    }
                }).join();
        }, 3, 10, TimeUnit.SECONDS);
    }
}
