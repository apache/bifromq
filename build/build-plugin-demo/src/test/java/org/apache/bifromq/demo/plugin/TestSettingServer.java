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

package org.apache.bifromq.demo.plugin;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import lombok.SneakyThrows;
import org.apache.bifromq.plugin.settingprovider.Setting;

public class TestSettingServer {
    private static final Map<String, Object> settingMap = new HashMap<>();
    private final HttpServer server;

    @SneakyThrows
    public TestSettingServer() {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/query", new QueryHandler());
        server.setExecutor(null);
    }

    private static void sendResponse(HttpExchange exchange, String response) throws IOException {
        sendResponse(exchange, response, 200);
    }

    private static void sendResponse(HttpExchange exchange, String response, int statusCode) throws IOException {
        exchange.getResponseHeaders().add("Content-Type", "text/plain");
        exchange.sendResponseHeaders(statusCode, response.getBytes().length);
        OutputStream os = exchange.getResponseBody();
        os.write(response.getBytes());
        os.close();
    }

    public void start() {
        server.start();
    }

    public void stop() {
        server.stop(0);
    }

    public URI getURI() {
        return URI.create(
            "http://" + server.getAddress().getHostName() + ":" + server.getAddress().getPort() + "/query");
    }

    public void provide(String tenantId, Setting setting, Object value) {
        settingMap.put(tenantId + setting.name(), value);
    }

    static class QueryHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if ("GET".equals(exchange.getRequestMethod())) {
                String tenantId = exchange.getRequestHeaders().getFirst("tenant_id");
                String settingName = exchange.getRequestHeaders().getFirst("setting_name");
                String key = tenantId + settingName;
                Object value = settingMap.get(key);
                if (value != null) {
                    sendResponse(exchange, value.toString());
                } else {
                    sendResponse(exchange, "", 404);
                }
            } else {
                sendResponse(exchange, "Method Not Allowed", 405);
            }
        }
    }
}
