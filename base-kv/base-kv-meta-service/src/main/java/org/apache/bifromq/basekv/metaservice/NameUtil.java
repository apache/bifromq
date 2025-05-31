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

package org.apache.bifromq.basekv.metaservice;

import org.apache.bifromq.basecrdt.core.api.CRDTURI;
import org.apache.bifromq.basecrdt.core.api.CausalCRDTType;

class NameUtil {
    private static final String PREFIX_LOAD_RULES = "loadRules-";
    private static final String PREFIX_LOAD_RULES_PROPOSAL = "loadRules-proposal-";
    private static final String PREFIX_LANDSCAPE = "landscape-";

    static String toLoadRulesURI(String clusterId) {
        return CRDTURI.toURI(CausalCRDTType.ormap, PREFIX_LOAD_RULES + clusterId);
    }

    static String toLoadRulesProposalURI(String clusterId) {
        return CRDTURI.toURI(CausalCRDTType.ormap, PREFIX_LOAD_RULES_PROPOSAL + clusterId);
    }

    static String toLandscapeURI(String clusterId) {
        return CRDTURI.toURI(CausalCRDTType.ormap, PREFIX_LANDSCAPE + clusterId);
    }

    static boolean isLandscapeURI(String crdtURI) {
        if (!CRDTURI.isValidURI(crdtURI)) {
            return false;
        }
        CausalCRDTType type = CRDTURI.parseType(crdtURI);
        String name = CRDTURI.parseName(crdtURI);
        return type == CausalCRDTType.ormap && name.startsWith(PREFIX_LANDSCAPE);
    }

    static String parseClusterId(String landscapeURI) {
        assert isLandscapeURI(landscapeURI);
        return CRDTURI.parseName(landscapeURI).substring(PREFIX_LANDSCAPE.length());
    }
}
