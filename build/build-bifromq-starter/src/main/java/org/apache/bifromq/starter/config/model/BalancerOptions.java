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

import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.protobuf.Struct;
import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.apache.bifromq.starter.config.model.serde.StructMapDeserializer;
import org.apache.bifromq.starter.config.model.serde.StructMapSerializer;

@Getter
@Setter
public class BalancerOptions {
    private long bootstrapDelayInMS = 15000;
    private long zombieProbeDelayInMS = 15000;
    private long retryDelayInMS = 5000;
    @JsonSetter(nulls = Nulls.SKIP)
    @JsonSerialize(using = StructMapSerializer.class)
    @JsonDeserialize(using = StructMapDeserializer.class)
    private Map<String, Struct> balancers = new HashMap<>();
}
