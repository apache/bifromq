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

package org.apache.bifromq.starter.config.model;

import static org.apache.bifromq.basekv.localengine.StructUtil.fromMap;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.protobuf.Struct;
import java.util.Map;
import java.util.TreeMap;
import org.apache.bifromq.starter.config.model.serde.EngineConfigDeserializer;
import org.apache.bifromq.starter.config.model.serde.EngineConfigSerializer;

@JsonSerialize(using = EngineConfigSerializer.class)
@JsonDeserialize(using = EngineConfigDeserializer.class)
public class EngineConfig extends TreeMap<String, Object> {
    private String type = "rocksdb";

    public String getType() {
        return type;
    }

    public EngineConfig setType(String type) {
        this.type = type;
        return this;
    }

    public EngineConfig setProps(Map<String, Object> props) {
        if (props != null) {
            this.putAll(props);
        }
        return this;
    }

    @JsonIgnore
    public Struct toStruct() {
        return fromMap(this);
    }
}
