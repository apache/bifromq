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

package org.apache.bifromq.basekv.store.util;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

public class ProcessUtil {
    private interface IProcessCpuLoad {
        double get();
    }

    private static final List<String> OPERATING_SYSTEM_BEAN_CLASS_NAMES = Arrays.asList(
        "com.ibm.lang.management.OperatingSystemMXBean", // J9
        "com.sun.management.OperatingSystemMXBean" // HotSpot
    );

    private static final List<String> POSSIBLE_METHODS =
        Arrays.asList("getProcessCpuLoad", "getSystemCpuLoad", "getCpuLoad");

    private static final IProcessCpuLoad CPU_LOAD;

    static {
        OperatingSystemMXBean operatingSystemBean = ManagementFactory.getOperatingSystemMXBean();
        Method method = null;
        for (String className : OPERATING_SYSTEM_BEAN_CLASS_NAMES) {
            try {
                Class<?> osBeanClass = Class.forName(className);
                // ensure the Bean we have is actually an instance of the interface
                osBeanClass.cast(operatingSystemBean);
                method = findMethod(osBeanClass);
                break;
            } catch (Throwable ignore) {
            }
        }
        if (method != null) {
            IProcessCpuLoad finalMethod = null;
            try {
                double load = (double) method.invoke(operatingSystemBean);
                finalMethod = new ProcessCpuLoad(operatingSystemBean, method);
            } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
                finalMethod = () -> Double.NaN;
            } finally {
                CPU_LOAD = finalMethod;
            }
        } else {
            CPU_LOAD = () -> Double.NaN;
        }
    }

    public static double cpuLoad() {
        return CPU_LOAD.get();
    }

    public static String processId() {
        return ManagementFactory.getRuntimeMXBean().getName();
    }

    private static class ProcessCpuLoad implements IProcessCpuLoad {
        private final OperatingSystemMXBean operatingSystemBean;

        private final Method cpuUsageMethod;

        private ProcessCpuLoad(OperatingSystemMXBean operatingSystemBean, Method cpuUsageMethod) {
            this.operatingSystemBean = operatingSystemBean;
            this.cpuUsageMethod = cpuUsageMethod;
        }


        public double get() {
            try {
                double load = (double) cpuUsageMethod.invoke(operatingSystemBean);
                return load > 1.0 ? load / 100 : load;
            } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
                return Double.NaN;
            }
        }
    }

    private static Method findMethod(Class<?> clazz) {
        for (String methodName : POSSIBLE_METHODS) {
            try {
                return clazz.getMethod(methodName);
            } catch (ClassCastException | NoSuchMethodException | SecurityException e) {
            }
        }
        return null;
    }
}
