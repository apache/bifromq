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

package org.apache.bifromq.basescheduler;

import static org.awaitility.Awaitility.await;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.time.Duration;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.Test;

@Slf4j
public class MovingAverageTest {
    @Test
    public void baseCase() {
        MovingAverage movingAverage = new MovingAverage(1, Duration.ofMillis(100));
        assertEquals(movingAverage.estimate(), 0);
        movingAverage.observe(10);
        assertEquals(movingAverage.estimate(), 10);
        assertEquals(movingAverage.max(), 10);
        movingAverage.observe(20);
        assertEquals(movingAverage.estimate(), 20);
        assertEquals(movingAverage.max(), 20);
        await().until(() -> movingAverage.estimate() == 0L);
    }

    @Test
    public void dropOldestObservation() {
        MovingAverage movingAverage = new MovingAverage(2, Duration.ofSeconds(60));
        movingAverage.observe(10);
        movingAverage.observe(20);
        assertEquals(movingAverage.estimate(), 15);
        assertEquals(movingAverage.max(), 20);

        movingAverage.observe(20);
        assertEquals(movingAverage.estimate(), 20);
        assertEquals(movingAverage.max(), 20);

        movingAverage.observe(30);
        assertEquals(movingAverage.estimate(), 25);
        assertEquals(movingAverage.max(), 30);
    }

    @Test
    public void dropStaledObservation() throws InterruptedException {
        MovingAverage movingAverage = new MovingAverage(3, Duration.ofMillis(10));
        movingAverage.observe(30);
        movingAverage.observe(30);
        movingAverage.observe(30);
        assertEquals(movingAverage.estimate(), 30);

        Thread.sleep(20);
        movingAverage.observe(40);
        movingAverage.observe(10);
        assertEquals(movingAverage.estimate(), 25);
    }

    @Test
    public void dataExpiration() throws InterruptedException {
        MovingAverage movingAverage = new MovingAverage(5, Duration.ofMillis(500));
        movingAverage.observe(100);
        movingAverage.observe(200);
        // Wait longer than freshness period
        Thread.sleep(600);
        movingAverage.observe(300);
        assertTrue(Math.abs(300 - movingAverage.estimate()) < 1);
        assertEquals(300, movingAverage.max());
    }
}
