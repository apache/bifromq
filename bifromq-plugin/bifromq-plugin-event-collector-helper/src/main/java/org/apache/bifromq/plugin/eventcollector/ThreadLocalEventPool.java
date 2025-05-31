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

package org.apache.bifromq.plugin.eventcollector;

import static java.lang.ThreadLocal.withInitial;

import java.lang.reflect.Modifier;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Set;
import lombok.SneakyThrows;
import org.reflections.Reflections;

public final class ThreadLocalEventPool {
    private static final Set<Class<? extends Event<?>>> EVENT_TYPES = new HashSet<>();

    private static final ThreadLocal<IdentityHashMap<Class<? extends Event<?>>, Event<?>>> THREAD_LOCAL_EVENTS;

    static {
        Reflections reflections = new Reflections(Event.class.getPackageName());
        for (Class<?> eventClass : reflections.getSubTypesOf(Event.class)) {
            if (!Modifier.isAbstract(eventClass.getModifiers())) {
                EVENT_TYPES.add((Class<? extends Event<?>>) eventClass);
            }
        }
        THREAD_LOCAL_EVENTS = withInitial(ThreadLocalEventPool::init);
    }

    private static IdentityHashMap<Class<? extends Event<?>>, Event<?>> init() {
        IdentityHashMap<Class<? extends Event<?>>, Event<?>> events = new IdentityHashMap<>(EVENT_TYPES.size());
        EVENT_TYPES.forEach(t -> add(t, events));
        return events;
    }

    @SneakyThrows
    private static void add(Class<? extends Event<?>> eventClass,
                            IdentityHashMap<Class<? extends Event<?>>, Event<?>> eventMap) {
        Event<?> event = eventClass.getConstructor().newInstance();
        eventMap.put(eventClass, event);
    }

    public static <T extends Event<T>> T getLocal(Class<T> eventClass) {
        return (T) THREAD_LOCAL_EVENTS.get().get(eventClass);
    }
}
