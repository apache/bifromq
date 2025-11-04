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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import org.apache.bifromq.starter.config.model.EngineConfig;

public class EngineConfigDeserializer extends StdDeserializer<EngineConfig> {
    public EngineConfigDeserializer() {
        super(EngineConfig.class);
    }

    @Override
    public EngineConfig deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        JsonNode node = p.getCodec().readTree(p);
        EngineConfig cfg = new EngineConfig();
        if (node.has("type") && node.get("type").isTextual()) {
            cfg.setType(node.get("type").asText());
        }
        Iterator<Map.Entry<String, JsonNode>> it = node.fields();
        while (it.hasNext()) {
            Map.Entry<String, JsonNode> e = it.next();
            String k = e.getKey();
            if ("type".equals(k)) {
                continue;
            }
            JsonNode v = e.getValue();
            cfg.put(k, toJava(v));
        }
        return cfg;
    }

    // Support in-place merge when @JsonMerge is present on the field
    @Override
    public EngineConfig deserialize(JsonParser p, DeserializationContext ctxt, EngineConfig intoValue)
        throws IOException {
        if (intoValue == null) {
            // Fallback to regular deserialization
            return deserialize(p, ctxt);
        }
        JsonNode node = p.getCodec().readTree(p);
        if (node.has("type") && node.get("type").isTextual()) {
            // Only override type when provided
            intoValue.setType(node.get("type").asText());
        }
        Iterator<Map.Entry<String, JsonNode>> it = node.fields();
        while (it.hasNext()) {
            Map.Entry<String, JsonNode> e = it.next();
            String k = e.getKey();
            if ("type".equals(k)) {
                continue;
            }
            JsonNode v = e.getValue();
            // Put to keep provided key and preserve others untouched
            intoValue.put(k, toJava(v));
        }
        return intoValue;
    }

    private Object toJava(JsonNode v) {
        if (v == null || v.isNull()) {
            return null;
        }
        if (v.isBoolean()) {
            return v.booleanValue();
        }
        if (v.isInt()) {
            return v.intValue();
        }
        if (v.isLong()) {
            return v.longValue();
        }
        if (v.isFloatingPointNumber()) {
            return v.doubleValue();
        }
        if (v.isTextual()) {
            return v.textValue();
        }
        // fallback to tree for complex types
        return v.toString();
    }
}
