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

package org.apache.bifromq.starter.config.model.serde;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import java.io.IOException;
import java.util.Map;
import org.apache.bifromq.starter.config.model.EngineConfig;

// Custom serializer to include standalone 'type' and map entries together
public class EngineConfigSerializer extends StdSerializer<EngineConfig> {
    public EngineConfigSerializer() {
        super(EngineConfig.class);
    }

    @Override
    public void serialize(EngineConfig value, JsonGenerator gen, SerializerProvider provider) throws IOException {
        gen.writeStartObject();
        String type = value.getType();
        gen.writeStringField("type", type);
        // write map entries following TreeMap order, skip 'type' key if present
        for (Map.Entry<String, Object> e : value.entrySet()) {
            String k = e.getKey();
            if ("type".equals(k)) {
                continue;
            }
            writeField(gen, k, e.getValue());
        }
        gen.writeEndObject();
    }

    private void writeField(JsonGenerator gen, String key, Object v) throws IOException {
        if (v == null) {
            gen.writeNullField(key);
            return;
        }
        if (v instanceof Boolean b) {
            gen.writeBooleanField(key, b);
            return;
        }
        if (v instanceof Number n) {
            double d = n.doubleValue();
            long l = (long) d;
            if (Double.isFinite(d) && d == l) {
                if (l >= Integer.MIN_VALUE && l <= Integer.MAX_VALUE) {
                    gen.writeNumberField(key, (int) l);
                } else {
                    gen.writeNumberField(key, l);
                }
            } else {
                gen.writeNumberField(key, d);
            }
            return;
        }
        gen.writeObjectField(key, v);
    }
}

