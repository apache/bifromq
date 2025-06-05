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

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;

public class UTF8Util {
    private static final int MAX_STRING_LENGTH = 65535; // MQTT-1.5.3

    public static boolean isWellFormed(String str, boolean sanityCheck) {
        int len = str.length();
        if (len == 0) {
            return true;
        }
        if (len > MAX_STRING_LENGTH) {
            return false;
        }
        char cl = str.charAt(0);
        if (cl == '\u0000') {
            return false;
        }
        if (sanityCheck && isUnacceptableChar(cl)) {
            return false;
        }
        for (int i = 1; i < len; i++) {
            final char cr = str.charAt(i);
            if (cr == '\u0000') {
                return false;
            }
            if (Character.isSurrogatePair(cl, cr)) {
                return false;
            }
            if (sanityCheck && isUnacceptableChar(cr)) {
                return false;
            }
            cl = cr;
        }
        return true;
    }

    public static boolean isValidUTF8Payload(ByteBuffer payload) {
        CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder();
        try {
            CharBuffer charBuffer = decoder.decode(payload);
            for (int i = 0; i < charBuffer.length(); i++) {
                char ch = charBuffer.get(i);
                if (isUnacceptableChar(ch)) {
                    return false;
                }
            }
        } catch (Throwable e) {
            return false;
        }
        return true;
    }

    public static boolean isValidUTF8Payload(byte[] payload) {
        return isValidUTF8Payload(ByteBuffer.wrap(payload));
    }

    private static boolean isUnacceptableChar(char ch) {
        return Character.isISOControl(ch) || !Character.isDefined(ch);
    }
}
