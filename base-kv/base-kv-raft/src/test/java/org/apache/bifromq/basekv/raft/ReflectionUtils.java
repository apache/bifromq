/*
 * Copyright (c) 2023. The BifroMQ Authors. All Rights Reserved.
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

package org.apache.bifromq.basekv.raft;

import java.lang.reflect.Field;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ReflectionUtils {
    @SuppressWarnings("unchecked")
    public static <T> T getField(Object targetObject, String fieldName) {
        try {
            Field field = targetObject.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            return (T) field.get(targetObject);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            log.warn("get field {} from {} failed: {}", targetObject, fieldName, e.getMessage());
        }
        return null;
    }
}