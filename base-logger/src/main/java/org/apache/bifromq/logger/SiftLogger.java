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

package org.apache.bifromq.logger;

import org.slf4j.Logger;
import org.slf4j.MDC;
import org.slf4j.Marker;

/**
 * SiftLogger is a logger that can log with sift key.
 */
public class SiftLogger extends FormatableLogger {
    private static final String SIFT_KEY = "sift_key";

    public static Logger getLogger(Class<?> clazz, String... tags) {
        return getLogger(clazz.getName(), tags);
    }

    public static Logger getLogger(String name, String... tags) {
        return new SiftLogger(name, tags);
    }

    private final String discriminatingValue;

    protected SiftLogger(Class<?> clazz, String... tags) {
        this(clazz.getName(), tags);
    }

    protected SiftLogger(String name, String... tags) {
        super(name);
        // put tags into mdc
        discriminatingValue = SiftKeyUtil.buildSiftKey(tags);
    }

    protected void doLog(LogMsg logFunc, String msg) {
        MDC.put(SIFT_KEY, discriminatingValue);
        super.doLog(logFunc, msg);
        MDC.remove(SIFT_KEY);
    }

    protected void doLog(LogFormatAndArg logFunc, String format, Object arg) {
        MDC.put(SIFT_KEY, discriminatingValue);
        super.doLog(logFunc, format, arg);
        MDC.remove(SIFT_KEY);
    }

    protected void doLog(LogFormatAndArg1Arg2 logFunc, String format, Object arg1, Object arg2) {
        MDC.put(SIFT_KEY, discriminatingValue);
        super.doLog(logFunc, format, arg1, arg2);
        MDC.remove(SIFT_KEY);
    }

    protected void doLogVarArgs(LogFormatAndVarArgs logFunc, String format, Object... arguments) {
        MDC.put(SIFT_KEY, discriminatingValue);
        super.doLogVarArgs(logFunc, format, arguments);
        MDC.remove(SIFT_KEY);
    }

    protected void doLogThrowable(LogMsgAndThrowable logFunc, String msg, Throwable t) {
        MDC.put(SIFT_KEY, discriminatingValue);
        super.doLogThrowable(logFunc, msg, t);
        MDC.remove(SIFT_KEY);
    }

    protected void doLog(LogMarkerMsg logFunc, Marker marker, String msg) {
        MDC.put(SIFT_KEY, discriminatingValue);
        super.doLog(logFunc, marker, msg);
        MDC.remove(SIFT_KEY);
    }

    protected void doLog(LogMarkerFormatAndArg logFunc, Marker marker, String format, Object arg) {
        MDC.put(SIFT_KEY, discriminatingValue);
        super.doLog(logFunc, marker, format, arg);
        MDC.remove(SIFT_KEY);
    }

    protected void doLog(LogMarkerFormatAndArg1Arg2 logFunc, Marker marker, String format, Object arg1, Object arg2) {
        MDC.put(SIFT_KEY, discriminatingValue);
        super.doLog(logFunc, marker, format, arg1, arg2);
        MDC.remove(SIFT_KEY);
    }

    protected void doLogVarArgs(LogMarkerFormatAndVarArgs logFunc, Marker marker, String format, Object... arguments) {
        MDC.put(SIFT_KEY, discriminatingValue);
        super.doLogVarArgs(logFunc, marker, format, arguments);
        MDC.remove(SIFT_KEY);
    }

    protected void doLogThrowable(LogMarkerMsgAndThrowable logFunc, Marker marker, String msg, Throwable t) {
        MDC.put(SIFT_KEY, discriminatingValue);
        super.doLogThrowable(logFunc, marker, msg, t);
        MDC.remove(SIFT_KEY);
    }
}
