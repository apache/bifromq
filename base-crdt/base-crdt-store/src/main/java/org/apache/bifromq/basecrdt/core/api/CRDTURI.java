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

package org.apache.bifromq.basecrdt.core.api;

import com.google.common.base.Preconditions;
import java.util.regex.Pattern;

public final class CRDTURI {
    private static final Pattern NAME;
    private static final Pattern URI;

    static {
        String namePattern = "[\\w-:.@]+";
        StringBuilder uriPattern = new StringBuilder("^(");
        for (CausalCRDTType type : CausalCRDTType.values()) {
            uriPattern.append(type.name());
            uriPattern.append("|");
        }
        uriPattern.deleteCharAt(uriPattern.length() - 1);
        uriPattern.append("):").append(namePattern);
        NAME = Pattern.compile(namePattern);
        URI = Pattern.compile(uriPattern.toString());
    }

    public static boolean isValidURI(String uri) {
        return URI.matcher(uri).matches();
    }

    public static String toURI(CausalCRDTType type, String name) {
        Preconditions.checkArgument(NAME.matcher(name).matches(), "Invalid name " + name);
        return type.name() + ":" + name;
    }

    public static CausalCRDTType parseType(String uri) {
        checkURI(uri);
        return CausalCRDTType.valueOf(uri.split(":")[0]);
    }

    public static String parseName(String uri) {
        checkURI(uri);
        return uri.substring(uri.indexOf(":") + 1);
    }

    public static void checkURI(String uri) {
        Preconditions.checkArgument(isValidURI(uri), "Invalid CRDT replica uri format");
    }
}
