/*
 * Copyright (c) 2024. The BifroMQ Authors. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.bifromq.demo.plugin;

import com.google.protobuf.util.JsonFormat;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import lombok.SneakyThrows;
import org.apache.bifromq.plugin.authprovider.type.MQTT3AuthData;
import org.apache.bifromq.plugin.authprovider.type.MQTT3AuthResult;
import org.apache.bifromq.plugin.authprovider.type.MQTTAction;
import org.apache.bifromq.plugin.authprovider.type.Ok;
import org.apache.bifromq.plugin.authprovider.type.Reject;

public class TestAuthServer {
    private static final Set<String> authedUsers = new HashSet<>();
    private static final Set<String> permittedPubTopics = new HashSet<>();
    private static final Set<String> permittedSubTopicFilters = new HashSet<>();
    private final HttpServer server;

    @SneakyThrows
    public TestAuthServer() {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/auth", new AuthHandler());
        server.createContext("/check", new CheckHandler());
        server.setExecutor(null);
    }

    private static void sendJson(HttpExchange exchange, String response) throws IOException {
        sendResponse(exchange, "application/json", response, 200);
    }

    private static void sendText(HttpExchange exchange, String response) throws IOException {
        sendResponse(exchange, "text/plain", response, 200);
    }

    private static void sendResponse(HttpExchange exchange, String contentType, String response, int statusCode)
        throws IOException {
        exchange.getResponseHeaders().add("Content-Type", contentType);
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
            "http://" + server.getAddress().getHostName() + ":" + server.getAddress().getPort());
    }

    public void addAuthedUser(String username) {
        authedUsers.add(username);
    }

    public void addPermittedPubTopic(String topic) {
        permittedPubTopics.add(topic);
    }

    public void addPermittedSubTopicFilter(String topicFilter) {
        permittedSubTopicFilters.add(topicFilter);
    }

    static class AuthHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if ("POST".equals(exchange.getRequestMethod())) {
                // read request body as JSON string
                String requestBody = new String(exchange.getRequestBody().readAllBytes());
                MQTT3AuthData.Builder authDataBuilder = MQTT3AuthData.newBuilder();
                JsonFormat.parser().merge(requestBody, authDataBuilder);
                MQTT3AuthData authData = authDataBuilder.build();
                if (authedUsers.contains(authData.getUsername())) {
                    sendJson(exchange, JsonFormat.printer().print(MQTT3AuthResult.newBuilder()
                        .setOk(Ok.newBuilder()
                            .setTenantId("TestTenant")
                            .setUserId(authData.getUsername())
                            .build())
                        .build()));
                } else {
                    sendJson(exchange, JsonFormat.printer().print(MQTT3AuthResult.newBuilder()
                        .setReject(Reject.newBuilder()
                            .setCode(Reject.Code.NotAuthorized)
                            .setTenantId("TestTenant")
                            .setUserId(authData.getUsername())
                            .build())
                        .build()));
                }
            } else {
                sendResponse(exchange, "text/plain", "Method Not Allowed", 405);
            }
        }
    }

    static class CheckHandler implements HttpHandler {

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if ("POST".equals(exchange.getRequestMethod())) {
                // read request body as JSON string
                String requestBody = new String(exchange.getRequestBody().readAllBytes());
                MQTTAction.Builder mqttActionBuilder = MQTTAction.newBuilder();
                JsonFormat.parser().merge(requestBody, mqttActionBuilder);
                MQTTAction mqttAction = mqttActionBuilder.build();
                switch (mqttAction.getTypeCase()) {
                    case PUB -> sendText(exchange,
                        Boolean.toString(permittedPubTopics.contains(mqttAction.getPub().getTopic())));
                    case SUB, UNSUB -> sendText(exchange,
                        Boolean.toString(permittedSubTopicFilters.contains(mqttAction.getPub().getTopic())));
                }
            } else {
                sendResponse(exchange, "text/plain", "Method Not Allowed", 405);
            }
        }
    }
}
