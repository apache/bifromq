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
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.protobuf.Struct;
import com.google.protobuf.util.JsonFormat;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Deserializes a JSON object or array into a Map of String to Struct.
 */
public class StructMapDeserializer extends JsonDeserializer<Map<String, Struct>> {
    @Override
    public Map<String, Struct> deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        JsonNode node = p.getCodec().readTree(p);
        Map<String, Struct> structMap = new HashMap<>();

        if (node.isArray()) {
            for (JsonNode subNode : node) {
                String key = subNode.asText();
                structMap.put(key, Struct.getDefaultInstance());
            }
        } else if (node.isObject()) {
            Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> field = fields.next();
                String key = field.getKey();
                JsonNode structJson = field.getValue();
                String jsonString = structJson.toString();
                Struct.Builder structBuilder = Struct.newBuilder();
                JsonFormat.parser().merge(jsonString, structBuilder);
                Struct struct = structBuilder.build();
                structMap.put(key, struct);
            }
        }
        return structMap;
    }
}
