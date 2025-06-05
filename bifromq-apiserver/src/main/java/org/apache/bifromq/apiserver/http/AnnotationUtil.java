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

package org.apache.bifromq.apiserver.http;

import io.netty.handler.codec.http.HttpMethod;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HEAD;
import jakarta.ws.rs.OPTIONS;
import jakarta.ws.rs.PATCH;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import java.lang.reflect.Method;

public class AnnotationUtil {
    public static <T extends IHTTPRequestHandler> HttpMethod getHTTPMethod(Class<T> handlerClass) {
        for (Method handleMethod : handlerClass.getMethods()) {
            if (handleMethod.getAnnotation(GET.class) != null) {
                return HttpMethod.GET;
            } else if (handleMethod.getAnnotation(PUT.class) != null) {
                return HttpMethod.PUT;
            } else if (handleMethod.getAnnotation(POST.class) != null) {
                return HttpMethod.POST;
            } else if (handleMethod.getAnnotation(DELETE.class) != null) {
                return HttpMethod.DELETE;
            } else if (handleMethod.getAnnotation(OPTIONS.class) != null) {
                return HttpMethod.OPTIONS;
            } else if (handleMethod.getAnnotation(HEAD.class) != null) {
                return HttpMethod.HEAD;
            } else if (handleMethod.getAnnotation(PATCH.class) != null) {
                return HttpMethod.PATCH;
            }
        }
        return null;
    }

    public static <T extends IHTTPRequestHandler> Path getPath(Class<T> handlerClass) {
        return handlerClass.getAnnotation(Path.class);
    }
}
