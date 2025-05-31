/*
 * Copyright (c) 2024. The BifroMQ Authors. All Rights Reserved.
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

package org.apache.bifromq.mqtt.handler.v5;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SenderTopicAliasManagerTest {

    @BeforeMethod
    public void setUp() {
    }

    @Test
    public void aliasAssignmentOnFirstRequest() {
        SenderTopicAliasManager aliasManager = new SenderTopicAliasManager(10, Duration.ofSeconds(1));
        Optional<SenderTopicAliasManager.AliasCreationResult> result = aliasManager.tryAlias("topic1");
        assertTrue(result.isPresent());
        assertEquals(result.get().alias(), 1);
        assertTrue(result.get().isFirstTime());
    }

    @Test
    public void aliasReuseForExistingTopic() {
        SenderTopicAliasManager aliasManager = new SenderTopicAliasManager(10, Duration.ofSeconds(1));
        aliasManager.tryAlias("topic1");
        Optional<SenderTopicAliasManager.AliasCreationResult> result =
            aliasManager.tryAlias("topic1");
        assertTrue(result.isPresent());
        assertEquals(result.get().alias(), 1);
        assertFalse(result.get().isFirstTime());
    }

    @Test
    public void aliasRecyclingWhenMaxReached() {
        SenderTopicAliasManager aliasManager = new SenderTopicAliasManager(1, Duration.ofSeconds(1));
        // Initialize aliasManager with a smaller maxAlias for testing
        aliasManager.tryAlias("topic1"); // Assign the only alias available
        Optional<SenderTopicAliasManager.AliasCreationResult> result =
            aliasManager.tryAlias("topic2"); // Attempt to assign an alias to a new topic, triggering recycling
        assertTrue(result.isPresent());
        assertEquals(result.get().alias(), 1); // Expect the alias to be recycled
        assertTrue(result.get().isFirstTime());
    }

    @Test
    public void maxAliasIsZero() {
        SenderTopicAliasManager aliasManager = new SenderTopicAliasManager(0, Duration.ofSeconds(1));
        Optional<SenderTopicAliasManager.AliasCreationResult> result = aliasManager.tryAlias("topic1");
        assertFalse(result.isPresent());
    }

    @Test
    public void testAliasOverflow() {
        SenderTopicAliasManager aliasManager =
            new SenderTopicAliasManager(2, Duration.ofSeconds(1)); // Set maxAlias to 2 for simplicity
        aliasManager.tryAlias("topic1");
        aliasManager.tryAlias("topic2");
        // This should trigger alias overflow
        Optional<SenderTopicAliasManager.AliasCreationResult> result = aliasManager.tryAlias("topic3");
        assertTrue(result.isPresent());
        // Assuming alias wraps back to 1 after reaching maxAlias
        assertEquals(result.get().alias(), 1);
        assertTrue(result.get().isFirstTime());
    }

    @Test
    public void testCacheExpiry() throws InterruptedException {
        SenderTopicAliasManager aliasManager =
            new SenderTopicAliasManager(10, Duration.ofMillis(100)); // Set a short expiry for testing
        aliasManager.tryAlias("topic1");
        // Wait for cache to expire
        TimeUnit.MILLISECONDS.sleep(200);
        // Attempt to reuse the alias after expiry
        Optional<SenderTopicAliasManager.AliasCreationResult> result = aliasManager.tryAlias("topic1");
        assertTrue(result.isPresent());
        // If the cache has expired, the topic should be treated as new, hence first time
        assertTrue(result.get().isFirstTime());
    }
}