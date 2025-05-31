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

package org.apache.bifromq.baserpc.trafficgovernor;

import org.apache.bifromq.basecrdt.core.api.CRDTURI;
import org.apache.bifromq.basecrdt.core.api.CausalCRDTType;

public class NameUtil {
    private static final String PREFIX_RPC = "RPC:";

    static String toLandscapeURI(String serviceUniqueName) {
        return CRDTURI.toURI(CausalCRDTType.ormap, PREFIX_RPC + serviceUniqueName);
    }

    static boolean isLandscapeURI(String crdtURI) {
        if (!CRDTURI.isValidURI(crdtURI)) {
            return false;
        }
        CausalCRDTType type = CRDTURI.parseType(crdtURI);
        String name = CRDTURI.parseName(crdtURI);
        return type == CausalCRDTType.ormap && name.startsWith(PREFIX_RPC);
    }

    static String parseServiceUniqueName(String landscapeURI) {
        assert isLandscapeURI(landscapeURI);
        return CRDTURI.parseName(landscapeURI).substring(PREFIX_RPC.length());
    }
}
