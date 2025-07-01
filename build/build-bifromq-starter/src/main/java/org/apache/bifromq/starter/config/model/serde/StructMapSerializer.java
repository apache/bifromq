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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.google.protobuf.Struct;
import com.google.protobuf.util.JsonFormat;
import java.io.IOException;
import java.util.Map;

/**
 * Serializes a Map of String to Struct into a JSON object.
 */
public class StructMapSerializer extends JsonSerializer<Map<String, Struct>> {
    @Override
    public void serialize(Map<String, Struct> value, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
        gen.writeStartObject();
        for (Map.Entry<String, Struct> entry : value.entrySet()) {
            String key = entry.getKey();
            Struct struct = entry.getValue();
            gen.writeFieldName(key);
            String jsonString = JsonFormat.printer().print(struct);
            JsonNode jsonNode = gen.getCodec().readTree(gen.getCodec().getFactory().createParser(jsonString));
            gen.writeTree(jsonNode);
        }
        gen.writeEndObject();
    }
}
