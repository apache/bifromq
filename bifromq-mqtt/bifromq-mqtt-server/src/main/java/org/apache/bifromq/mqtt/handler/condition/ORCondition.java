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

package org.apache.bifromq.mqtt.handler.condition;

/**
 * Group of conditions that are met if any of the conditions are met.
 */
public class ORCondition implements Condition {
    private final Condition[] conditions;
    private String message;

    public static Condition or(Condition... conditions) {
        return new ORCondition(conditions);
    }

    public ORCondition(Condition... conditions) {
        this.conditions = conditions;
    }

    @Override
    public boolean meet() {
        for (Condition condition : conditions) {
            if (condition.meet()) {
                message = condition.toString();
                return true;
            }
        }
        message = "";
        return false;
    }

    @Override
    public String toString() {
        return message;
    }
}
