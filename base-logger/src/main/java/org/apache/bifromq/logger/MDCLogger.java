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

package org.apache.bifromq.logger;

import static java.util.Collections.emptyMap;

import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.slf4j.Marker;
import org.slf4j.event.Level;
import org.slf4j.spi.LocationAwareLogger;

/**
 * A logger that supports adding multiple key-value tags to the log entries.
 */
public class MDCLogger implements org.slf4j.Logger {
    private static final Optional<Level> EMPTY_LEVEL = Optional.empty();
    private static final Optional<Level> TRACE_LEVEL = Optional.of(Level.TRACE);
    private static final Optional<Level> DEBUG_LEVEL = Optional.of(Level.DEBUG);
    private static final Optional<Level> INFO_LEVEL = Optional.of(Level.INFO);
    private static final Optional<Level> WARN_LEVEL = Optional.of(Level.WARN);
    private static final Optional<Level> ERROR_LEVEL = Optional.of(Level.ERROR);
    private static final String FQCN = MDCLogger.class.getName();
    private final LocationAwareLogger delegate;
    private final String[] tags;

    /**
     * Creates a new MDCLogger with the specified name and key-value tags.
     *
     * @param name the name of the logger
     * @param tags key-value pairs to be added to the MDC context
     */
    public MDCLogger(String name, String... tags) {
        assert tags.length % 2 == 0 : "Tags must be in key-value pairs";
        this.delegate = (LocationAwareLogger) LoggerFactory.getLogger(name);
        this.tags = tags;
    }

    public static org.slf4j.Logger getLogger(Class<?> clazz, String... tags) {
        return getLogger(clazz.getName(), tags);
    }

    public static org.slf4j.Logger getLogger(String name, String... tags) {
        return new MDCLogger(name, tags);
    }

    protected Map<String, String> extraContext() {
        return emptyMap();
    }

    private void logWithMDC(Supplier<Optional<Level>> isEnabled,
                            Marker marker, String msg, Object[] args, Throwable t) {
        Optional<Level> lvl = isEnabled.get();
        if (lvl.isEmpty()) {
            return;
        }
        Object[] evaluated = args;
        if (args != null && args.length > 0) {
            evaluated = new Object[args.length];
            for (int i = 0; i < args.length; i++) {
                Object a = args[i];
                evaluated[i] = (a instanceof Supplier) ? ((Supplier<?>) a).get() : a;
            }
        }
        for (int i = 0; i < tags.length; i += 2) {
            MDC.put(tags[i], tags[i + 1]);
        }
        Map<String, String> extraCtx = extraContext();
        extraCtx.forEach(MDC::put);
        delegate.log(marker, FQCN, lvl.get().toInt(), msg, evaluated, t);
        for (int i = 0; i < tags.length; i += 2) {
            MDC.remove(tags[i]);
        }
        extraCtx.keySet().forEach(MDC::remove);
    }

    @Override
    public String getName() {
        return delegate.getName();
    }

    @Override
    public boolean isTraceEnabled() {
        return delegate.isTraceEnabled();
    }

    private Optional<Level> isTraceEnabledSupplier() {
        return delegate.isTraceEnabled() ? TRACE_LEVEL : EMPTY_LEVEL;
    }

    @Override
    public void trace(String msg) {
        logWithMDC(this::isTraceEnabledSupplier, null, msg, null, null);
    }

    @Override
    public void trace(String format, Object arg) {
        logWithMDC(this::isTraceEnabledSupplier, null, format, new Object[] {arg}, null);
    }

    @Override
    public void trace(String format, Object arg1, Object arg2) {
        logWithMDC(this::isTraceEnabledSupplier, null, format, new Object[] {arg1, arg2}, null);
    }

    @Override
    public void trace(String format, Object... arguments) {
        logWithMDC(this::isTraceEnabledSupplier, null, format, arguments, null);
    }

    @Override
    public void trace(String msg, Throwable t) {
        logWithMDC(this::isTraceEnabledSupplier, null, msg, null, t);
    }

    @Override
    public boolean isTraceEnabled(Marker marker) {
        return delegate.isTraceEnabled(marker);
    }

    private Supplier<Optional<Level>> isTraceEnabledSupplier(Marker marker) {
        return () -> delegate.isTraceEnabled(marker) ? TRACE_LEVEL : EMPTY_LEVEL;
    }

    @Override
    public void trace(Marker marker, String msg) {
        logWithMDC(this.isTraceEnabledSupplier(marker), marker, msg, null, null);
    }

    @Override
    public void trace(Marker marker, String format, Object arg) {
        logWithMDC(this.isTraceEnabledSupplier(marker), marker, format, new Object[] {arg}, null);
    }

    @Override
    public void trace(Marker marker, String format, Object arg1, Object arg2) {
        logWithMDC(this.isTraceEnabledSupplier(marker), marker, format, new Object[] {arg1, arg2}, null);
    }

    @Override
    public void trace(Marker marker, String format, Object... argArray) {
        logWithMDC(this.isTraceEnabledSupplier(marker), marker, format, argArray, null);
    }

    @Override
    public void trace(Marker marker, String msg, Throwable t) {
        logWithMDC(this.isTraceEnabledSupplier(marker), marker, msg, null, t);
    }

    @Override
    public boolean isDebugEnabled() {
        return delegate.isDebugEnabled();
    }

    private Optional<Level> isDebugEnabledSupplier() {
        return delegate.isDebugEnabled() ? DEBUG_LEVEL : EMPTY_LEVEL;
    }

    @Override
    public void debug(String msg) {
        logWithMDC(this::isDebugEnabledSupplier, null, msg, null, null);
    }

    @Override
    public void debug(String format, Object arg) {
        logWithMDC(this::isDebugEnabledSupplier, null, format, new Object[] {arg}, null);
    }

    @Override
    public void debug(String format, Object arg1, Object arg2) {
        logWithMDC(this::isDebugEnabledSupplier, null, format, new Object[] {arg1, arg2}, null);
    }

    @Override
    public void debug(String format, Object... arguments) {
        logWithMDC(this::isDebugEnabledSupplier, null, format, arguments, null);
    }

    @Override
    public void debug(String msg, Throwable t) {
        logWithMDC(this::isDebugEnabledSupplier, null, msg, null, t);
    }

    @Override
    public boolean isDebugEnabled(Marker marker) {
        return delegate.isDebugEnabled(marker);
    }

    private Supplier<Optional<Level>> isDebugEnabledSupplier(Marker marker) {
        return () -> delegate.isDebugEnabled(marker) ? DEBUG_LEVEL : EMPTY_LEVEL;
    }

    @Override
    public void debug(Marker marker, String msg) {
        logWithMDC(this.isDebugEnabledSupplier(marker), marker, msg, null, null);
    }

    @Override
    public void debug(Marker marker, String format, Object arg) {
        logWithMDC(this.isDebugEnabledSupplier(marker), marker, format, new Object[] {arg}, null);
    }

    @Override
    public void debug(Marker marker, String format, Object arg1, Object arg2) {
        logWithMDC(this.isDebugEnabledSupplier(marker), marker, format, new Object[] {arg1, arg2}, null);
    }

    @Override
    public void debug(Marker marker, String format, Object... arguments) {
        logWithMDC(this.isDebugEnabledSupplier(marker), marker, format, arguments, null);
    }

    @Override
    public void debug(Marker marker, String msg, Throwable t) {
        logWithMDC(this.isDebugEnabledSupplier(marker), marker, msg, null, t);
    }

    @Override
    public boolean isInfoEnabled() {
        return delegate.isInfoEnabled();
    }

    private Optional<Level> isInfoEnabledSupplier() {
        return delegate.isInfoEnabled() ? INFO_LEVEL : EMPTY_LEVEL;
    }

    @Override
    public void info(String msg) {
        logWithMDC(this::isInfoEnabledSupplier, null, msg, null, null);
    }

    @Override
    public void info(String format, Object arg) {
        logWithMDC(this::isInfoEnabledSupplier, null, format, new Object[] {arg}, null);
    }

    @Override
    public void info(String format, Object arg1, Object arg2) {
        logWithMDC(this::isInfoEnabledSupplier, null, format, new Object[] {arg1, arg2}, null);
    }

    @Override
    public void info(String format, Object... arguments) {
        logWithMDC(this::isInfoEnabledSupplier, null, format, arguments, null);
    }

    @Override
    public void info(String msg, Throwable t) {
        logWithMDC(this::isInfoEnabledSupplier, null, msg, null, t);
    }

    @Override
    public boolean isInfoEnabled(Marker marker) {
        return delegate.isInfoEnabled(marker);
    }

    private Supplier<Optional<Level>> isInfoEnabledSupplier(Marker marker) {
        return () -> delegate.isInfoEnabled(marker) ? INFO_LEVEL : EMPTY_LEVEL;
    }

    @Override
    public void info(Marker marker, String msg) {
        logWithMDC(this.isInfoEnabledSupplier(marker), marker, msg, null, null);
    }

    @Override
    public void info(Marker marker, String format, Object arg) {
        logWithMDC(this.isInfoEnabledSupplier(marker), marker, format, new Object[] {arg}, null);
    }

    @Override
    public void info(Marker marker, String format, Object arg1, Object arg2) {
        logWithMDC(this.isInfoEnabledSupplier(marker), marker, format, new Object[] {arg1, arg2}, null);
    }

    @Override
    public void info(Marker marker, String format, Object... arguments) {
        logWithMDC(this.isInfoEnabledSupplier(marker), marker, format, arguments, null);
    }

    @Override
    public void info(Marker marker, String msg, Throwable t) {
        logWithMDC(this.isInfoEnabledSupplier(marker), marker, msg, null, t);
    }

    @Override
    public boolean isWarnEnabled() {
        return delegate.isWarnEnabled();
    }

    private Optional<Level> isWarnEnabledSupplier() {
        return delegate.isWarnEnabled() ? WARN_LEVEL : EMPTY_LEVEL;
    }

    @Override
    public void warn(String msg) {
        logWithMDC(this::isWarnEnabledSupplier, null, msg, null, null);
    }

    @Override
    public void warn(String format, Object arg) {
        logWithMDC(this::isWarnEnabledSupplier, null, format, new Object[] {arg}, null);
    }

    @Override
    public void warn(String format, Object arg1, Object arg2) {
        logWithMDC(this::isWarnEnabledSupplier, null, format, new Object[] {arg1, arg2}, null);
    }

    @Override
    public void warn(String format, Object... arguments) {
        logWithMDC(this::isWarnEnabledSupplier, null, format, arguments, null);
    }

    @Override
    public void warn(String msg, Throwable t) {
        logWithMDC(this::isWarnEnabledSupplier, null, msg, null, t);
    }

    @Override
    public boolean isWarnEnabled(Marker marker) {
        return delegate.isWarnEnabled(marker);
    }

    private Supplier<Optional<Level>> isWarnEnabledSupplier(Marker marker) {
        return () -> delegate.isWarnEnabled(marker) ? WARN_LEVEL : EMPTY_LEVEL;
    }

    @Override
    public void warn(Marker marker, String msg) {
        logWithMDC(this.isWarnEnabledSupplier(marker), marker, msg, null, null);
    }

    @Override
    public void warn(Marker marker, String format, Object arg) {
        logWithMDC(this.isWarnEnabledSupplier(marker), marker, format, new Object[] {arg}, null);
    }

    @Override
    public void warn(Marker marker, String format, Object arg1, Object arg2) {
        logWithMDC(this.isWarnEnabledSupplier(marker), marker, format, new Object[] {arg1, arg2}, null);
    }

    @Override
    public void warn(Marker marker, String format, Object... arguments) {
        logWithMDC(this.isWarnEnabledSupplier(marker), marker, format, arguments, null);
    }

    @Override
    public void warn(Marker marker, String msg, Throwable t) {
        logWithMDC(this.isWarnEnabledSupplier(marker), marker, msg, null, t);
    }

    @Override
    public boolean isErrorEnabled() {
        return delegate.isErrorEnabled();
    }

    private Optional<Level> isErrorEnabledSupplier() {
        return delegate.isErrorEnabled() ? ERROR_LEVEL : EMPTY_LEVEL;
    }

    @Override
    public void error(String msg) {
        logWithMDC(this::isErrorEnabledSupplier, null, msg, null, null);
    }

    @Override
    public void error(String format, Object arg) {
        logWithMDC(this::isErrorEnabledSupplier, null, format, new Object[] {arg}, null);
    }

    @Override
    public void error(String format, Object arg1, Object arg2) {
        logWithMDC(this::isErrorEnabledSupplier, null, format, new Object[] {arg1, arg2}, null);
    }

    @Override
    public void error(String format, Object... arguments) {
        logWithMDC(this::isErrorEnabledSupplier, null, format, arguments, null);
    }

    @Override
    public void error(String msg, Throwable t) {
        logWithMDC(this::isErrorEnabledSupplier, null, msg, null, t);
    }

    @Override
    public boolean isErrorEnabled(Marker marker) {
        return delegate.isErrorEnabled(marker);
    }

    private Supplier<Optional<Level>> isErrorEnabledSupplier(Marker marker) {
        return () -> delegate.isErrorEnabled(marker) ? ERROR_LEVEL : EMPTY_LEVEL;
    }

    @Override
    public void error(Marker marker, String msg) {
        logWithMDC(this.isErrorEnabledSupplier(marker), marker, msg, null, null);
    }

    @Override
    public void error(Marker marker, String format, Object arg) {
        logWithMDC(this.isErrorEnabledSupplier(marker), marker, format, new Object[] {arg}, null);
    }

    @Override
    public void error(Marker marker, String format, Object arg1, Object arg2) {
        logWithMDC(this.isErrorEnabledSupplier(marker), marker, format, new Object[] {arg1, arg2}, null);
    }

    @Override
    public void error(Marker marker, String format, Object... arguments) {
        logWithMDC(this.isErrorEnabledSupplier(marker), marker, format, arguments, null);
    }

    @Override
    public void error(Marker marker, String msg, Throwable t) {
        logWithMDC(this.isErrorEnabledSupplier(marker), marker, msg, null, t);
    }
}