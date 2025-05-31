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

import org.apache.bifromq.basekv.balance.command.BalanceCommand;

/**
 * The balance command need to be run now.
 */
public final class BalanceNow<T extends BalanceCommand> implements BalanceResult {
    public final T command;

    private BalanceNow(T command) {
        this.command = command;
    }

    @Override
    public BalanceResultType type() {
        return BalanceResultType.BalanceNow;
    }

    public static <T extends BalanceCommand> BalanceNow<T> of(T command) {
        return new BalanceNow<>(command);
    }
}
