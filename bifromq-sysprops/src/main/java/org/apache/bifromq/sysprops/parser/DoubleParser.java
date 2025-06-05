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

public class DoubleParser implements PropParser<Double> {
    private final double lowBound;
    private final boolean excludeLowerBound;
    private final double highBoundEx;

    private DoubleParser(double lowBound, double highBoundEx, boolean excludeLowerBound) {
        this.lowBound = lowBound;
        this.highBoundEx = highBoundEx;
        this.excludeLowerBound = excludeLowerBound;
    }

    @Override
    public Double parse(String value) {
        double val = Double.parseDouble(value);
        int isLower = Double.compare(lowBound, val);
        if ((excludeLowerBound ? isLower < 0 : isLower <= 0) && Double.compare(val, highBoundEx) < 0) {
            return val;
        }
        if (excludeLowerBound) {
            throw new SysPropParseException(String.format("%f is out of bound (%f,%f)", val, lowBound, highBoundEx));
        } else {
            throw new SysPropParseException(String.format("%f is out of bound [%f,%f)", val, lowBound, highBoundEx));
        }
    }

    public static DoubleParser from(double lowBound, double highBoundEx, boolean excludeLowerBound) {
        assert Double.compare(lowBound, highBoundEx) < 0;
        return new DoubleParser(lowBound, highBoundEx, excludeLowerBound);
    }
}
