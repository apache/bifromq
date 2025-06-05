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

package org.apache.bifromq.mqtt.condition;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.apache.bifromq.mqtt.handler.condition.Condition;
import org.apache.bifromq.mqtt.handler.condition.ORCondition;
import org.testng.annotations.Test;


public class ORConditionTest {

    @Test
    public void testAllConditionsMet() {
        Condition condition1 = mock(Condition.class);
        Condition condition2 = mock(Condition.class);
        Condition condition3 = mock(Condition.class);

        when(condition1.meet()).thenReturn(true);
        when(condition2.meet()).thenReturn(true);
        when(condition3.meet()).thenReturn(true);

        ORCondition orCondition = new ORCondition(condition1, condition2, condition3);

        assertTrue(orCondition.meet());
        assertEquals(condition1.toString(), orCondition.toString());

        verify(condition1, times(1)).meet();
        verify(condition2, never()).meet();
        verify(condition3, never()).meet();
    }

    @Test
    public void testSomeConditionsMet() {
        // 创建三个模拟的 Condition 对象
        Condition condition1 = mock(Condition.class);
        Condition condition2 = mock(Condition.class);
        Condition condition3 = mock(Condition.class);

        when(condition1.meet()).thenReturn(false);
        when(condition2.meet()).thenReturn(true);
        when(condition3.meet()).thenReturn(false);

        ORCondition orCondition = new ORCondition(condition1, condition2, condition3);

        assertTrue(orCondition.meet());
        assertEquals(condition2.toString(), orCondition.toString());

        verify(condition1, times(1)).meet();
        verify(condition2, times(1)).meet();
        verify(condition3, never()).meet();
    }

    @Test
    public void testNoConditionsMet() {
        // 创建三个模拟的 Condition 对象
        Condition condition1 = mock(Condition.class);
        Condition condition2 = mock(Condition.class);
        Condition condition3 = mock(Condition.class);

        when(condition1.meet()).thenReturn(false);
        when(condition2.meet()).thenReturn(false);
        when(condition3.meet()).thenReturn(false);

        ORCondition orCondition = new ORCondition(condition1, condition2, condition3);

        assertFalse(orCondition.meet());
        assertEquals("", orCondition.toString());

        verify(condition1, times(1)).meet();
        verify(condition2, times(1)).meet();
        verify(condition3, times(1)).meet();
    }
}
