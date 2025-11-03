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

package org.apache.bifromq.basekv.localengine;

import com.google.protobuf.NullValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import java.util.Map;

public class StructUtil {
    public static Struct fromMap(Map<String, Object> config) {
        Struct.Builder b = Struct.newBuilder();
        config.forEach((k, v) -> b.putFields(k, toValue(v)));
        return b.build();
    }

    public static Value toValue(Object v) {
        if (v == null) {
            return Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build();
        }
        if (v instanceof Boolean) {
            return Value.newBuilder().setBoolValue((Boolean) v).build();
        }
        if (v instanceof Number) {
            return Value.newBuilder().setNumberValue(((Number) v).doubleValue()).build();
        }
        return Value.newBuilder().setStringValue(String.valueOf(v)).build();
    }

    public static Object fromValue(Value v) {
        switch (v.getKindCase()) {
            case NULL_VALUE -> {
                return null;
            }
            case BOOL_VALUE -> {
                return v.getBoolValue();
            }
            case NUMBER_VALUE -> {
                return coerceNumber(v.getNumberValue());
            }
            case STRING_VALUE -> {
                return v.getStringValue();
            }
            default -> {
                throw new UnsupportedOperationException("Unsupported engine config type: " + v.getKindCase());
            }
        }
    }

    public static boolean boolVal(Struct conf, String key) {
        return conf.getFieldsOrThrow(key).getBoolValue();
    }

    public static double numVal(Struct conf, String key) {
        return conf.getFieldsOrThrow(key).getNumberValue();
    }

    public static String strVal(Struct conf, String key) {
        return conf.getFieldsOrThrow(key).getStringValue();
    }

    private static Object coerceNumber(double d) {
        if (Double.isFinite(d) && d == Math.rint(d)) {
            if (d >= Integer.MIN_VALUE && d <= Integer.MAX_VALUE) {
                return (int) d;
            }
            if (d >= Long.MIN_VALUE && d <= Long.MAX_VALUE) {
                return (long) d;
            }
        }
        return d;
    }
}
