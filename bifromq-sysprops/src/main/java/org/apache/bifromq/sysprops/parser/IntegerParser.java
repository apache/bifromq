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

package org.apache.bifromq.sysprops.parser;

public class IntegerParser implements PropParser<Integer> {
    public static final IntegerParser POSITIVE = new IntegerParser(1, Integer.MAX_VALUE);
    public static final IntegerParser NON_NEGATIVE = new IntegerParser(0, Integer.MAX_VALUE);
    private final int lowBound;
    private final int highBoundEx;

    private IntegerParser(int lowBound, int highBoundEx) {
        this.lowBound = lowBound;
        this.highBoundEx = highBoundEx;
    }

    @Override
    public Integer parse(String value) {
        int val = lowBound >= 0 ? Integer.parseUnsignedInt(value) : Integer.parseInt(value);
        if (lowBound <= val && val < highBoundEx) {
            return val;
        }
        throw new SysPropParseException(String.format("%d is out of bound [%d,%d)", val, lowBound, highBoundEx));
    }

    public static IntegerParser from(int lowBound, int highBoundEx) {
        assert lowBound < highBoundEx;
        return new IntegerParser(lowBound, highBoundEx);
    }
}
