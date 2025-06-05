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

package org.apache.bifromq.apiserver;

public enum Headers {
    HEADER_REQ_ID("req_id"),
    HEADER_TENANT_ID("tenant_id"),
    HEADER_TOPIC("topic"),
    HEADER_TOPIC_FILTER("topic_filter"),
    HEADER_SUB_QOS("sub_qoS"),
    HEADER_CLIENT_TYPE("client_type"),
    HEADER_CLIENT_META_PREFIX("client_meta_"),
    HEADER_USER_ID("user_id"),
    HEADER_CLIENT_ID("client_id"),
    HEADER_SERVER_REDIRECT("server_redirect"),
    HEADER_SERVER_REFERENCE("server_reference"),
    HEADER_RETAIN("retain"),
    HEADER_QOS("qos"),
    HEADER_EXPIRY_SECONDS("expiry_seconds"),
    HEADER_SERVER_ID("server_id"),
    HEADER_SERVICE_NAME("service_name"),
    HEADER_STORE_NAME("store_name"),
    HEADER_BALANCER_FACTORY_CLASS("balancer_factory_class");

    public final String header;

    Headers(String header) {
        this.header = header;
    }

    @Override
    public String toString() {
        return header;
    }
}
