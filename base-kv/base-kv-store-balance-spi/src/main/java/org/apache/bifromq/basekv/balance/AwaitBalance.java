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

package org.apache.bifromq.basekv.balance;

import java.time.Duration;

/**
 * Await some time to generate balance command.
 */
public final class AwaitBalance implements BalanceResult {
    public final Duration await;

    private AwaitBalance(Duration await) {
        this.await = await;
    }

    @Override
    public BalanceResultType type() {
        return BalanceResultType.AwaitBalance;
    }

    public static AwaitBalance of(Duration await) {
        return new AwaitBalance(await);
    }
}
