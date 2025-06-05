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

import static org.apache.bifromq.type.MQTTClientInfoConstants.MQTT_USER_ID_KEY;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.apache.bifromq.plugin.authprovider.type.MQTT3AuthData;
import org.apache.bifromq.plugin.authprovider.type.MQTT3AuthResult;
import org.apache.bifromq.plugin.authprovider.type.MQTTAction;
import org.apache.bifromq.plugin.authprovider.type.PubAction;
import org.apache.bifromq.plugin.authprovider.type.SubAction;
import org.apache.bifromq.type.ClientInfo;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class WebHookBasedAuthProviderTest {
    private TestAuthServer testServer;

    @BeforeMethod
    private void setup() {
        testServer = new TestAuthServer();
        testServer.start();
    }

    @AfterMethod
    private void tearDown() {
        testServer.stop();
    }

    @Test
    public void testAuth() {
        MQTT3AuthData authedUser = MQTT3AuthData.newBuilder()
            .setUsername("authUser")
            .build();

        MQTT3AuthData unauthUser = MQTT3AuthData.newBuilder()
            .setUsername("unauthUser")
            .build();
        testServer.addAuthedUser(authedUser.getUsername());
        WebHookBasedAuthProvider provider = new WebHookBasedAuthProvider(testServer.getURI());
        MQTT3AuthResult authResult = provider.auth(authedUser).join();
        assertTrue(authResult.hasOk());

        authResult = provider.auth(unauthUser).join();
        assertFalse(authResult.hasOk());
    }

    @Test
    public void testCheck() {
        MQTTAction pubAction = MQTTAction.newBuilder()
            .setPub(PubAction.newBuilder().setTopic("PubTopic").build())
            .build();
        ClientInfo clientInfo = ClientInfo.newBuilder()
            .setTenantId("testTenant")
            .putMetadata(MQTT_USER_ID_KEY, "authUser")
            .build();
        testServer.addPermittedPubTopic(pubAction.getPub().getTopic());

        WebHookBasedAuthProvider provider = new WebHookBasedAuthProvider(testServer.getURI());
        assertTrue(provider.check(clientInfo, pubAction).join());
        assertFalse(provider.check(clientInfo, MQTTAction.newBuilder()
            .setSub(SubAction.newBuilder().setTopicFilter("SubTopic").build())
            .build()).join());
    }
}
