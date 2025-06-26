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

package org.apache.bifromq.starter.config.model.api;

import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;
import lombok.Getter;
import lombok.Setter;
import org.apache.bifromq.starter.config.model.ServerSSLContextConfig;

@Getter
@Setter
public class APIServerConfig {
    private boolean enable = true;
    private String host; // optional, if null host address will be used for listening api calls
    private int httpPort = 8091; // the listening port for http api
    private int maxContentLength = 256 * 1024;
    private int workerThreads = 2;
    private boolean enableSSL = false;
    @JsonSetter(nulls = Nulls.SKIP)
    private ServerSSLContextConfig sslConfig;
}
