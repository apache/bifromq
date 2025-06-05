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

package org.apache.bifromq.util;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.Test;

@Slf4j
public class UTF8UtilTest {
    @Test
    public void clientId() {
        for (int i = 0; i < 100; i++) {
            String s = UUID.randomUUID().toString();
            assertTrue(UTF8Util.isWellFormed(s, false));
        }
    }

    @Test
    public void emptyClientId() {
        assertTrue(UTF8Util.isWellFormed("", false));
        assertTrue(UTF8Util.isWellFormed(" ", false)); // whitespace is acceptable according to MQTT spec
    }

    @Test
    public void mustNotChars() {
        assertFalse(UTF8Util.isWellFormed("hello\u0000world", false)); // null character U+0000
        assertFalse(UTF8Util.isWellFormed("hello\uD83D\uDE0Aworld", false)); // surrogate pairs
    }

    @Test
    public void shouldNotChars() {
        for (int i = '\u0001'; i <= '\u001F'; i++) {
            assertFalse(UTF8Util.isWellFormed("hello" + (char) i, true)); // control character
        }
        for (int i = '\u007F'; i <= '\u009F'; i++) {
            assertFalse(UTF8Util.isWellFormed("hello" + (char) i, true)); // control character
        }
        assertFalse(UTF8Util.isWellFormed("hello\uFFFF", true)); // non character
    }

    @Test
    public void noSanityCheck() {
        for (int i = '\u0001'; i <= '\u001F'; i++) {
            assertTrue(UTF8Util.isWellFormed("hello" + (char) i, false)); // control character
        }
        for (int i = '\u007F'; i <= '\u009F'; i++) {
            assertTrue(UTF8Util.isWellFormed("hello" + (char) i, false)); // control character
        }
        assertTrue(UTF8Util.isWellFormed("hello\uFFFF", false)); // non character
    }

    @Test
    public void zeroWidthNoBreakSpace() {
        byte[] bytes = new byte[] {(byte) 0xEF, (byte) 0xBB, (byte) 0xBF};
        String s = new String(bytes, StandardCharsets.UTF_8);
        assertEquals(s.charAt(0), '\uFEFF'); // zero width no break space
    }
}
