/*
 * Copyright (c) 2025. The BifroMQ Authors. All Rights Reserved.
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

package org.apache.bifromq.test;

import java.util.HashSet;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.testng.IRetryAnalyzer;
import org.testng.ITestContext;
import org.testng.ITestListener;
import org.testng.ITestResult;

@Slf4j
public class RetryListener implements ITestListener {
    private final Set<ITestResult> retriedSuccess = new HashSet<>();

    @Override
    public void onTestSuccess(ITestResult result) {
        IRetryAnalyzer retryAnalyzer = result.getMethod().getRetryAnalyzer(result);
        if (retryAnalyzer instanceof RetryAnalyser) {
            if (((RetryAnalyser) retryAnalyzer).getRetryCount() > 0) {
                retriedSuccess.add(result);
                result.setAttribute("RETRIED_SUCCESS", true);
            }
        }
    }

    @Override
    public void onFinish(ITestContext context) {
        for (ITestResult result : retriedSuccess) {
            log.info("[RETRY SUCCESS] {}.{} passed after retry%n",
                result.getTestClass().getName(), result.getMethod().getMethodName());
        }
    }
}
