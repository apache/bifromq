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

package org.apache.bifromq.plugin.eventcollector;

import java.lang.reflect.Modifier;
import java.util.HashSet;
import java.util.Set;
import lombok.SneakyThrows;
import org.reflections.Reflections;

class EventPool {
    private static final Set<Class<Event<?>>> EVENT_TYPES = new HashSet<>();
    private final Event<?>[] events;

    static {
        Reflections reflections = new Reflections(Event.class.getPackageName());
        for (Class<?> eventClass : reflections.getSubTypesOf(Event.class)) {
            if (!Modifier.isAbstract(eventClass.getModifiers())) {
                EVENT_TYPES.add((Class<Event<?>>) eventClass);
            }
        }
    }

    EventPool() {
        events = new Event[EVENT_TYPES.size()];
        EVENT_TYPES.forEach(this::add);
    }

    <T extends Event<T>> T get(EventType eventType) {
        return (T) events[eventType.ordinal()];
    }

    @SneakyThrows
    private void add(Class<Event<?>> eventClass) {
        Event<?> event = eventClass.getConstructor().newInstance();
        events[event.type().ordinal()] = event;
    }
}
