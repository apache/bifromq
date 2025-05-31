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

package org.apache.bifromq.sessiondict.client;

import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.type.ClientInfo;

@Slf4j
class SessionRegistration implements ISessionRegistration {
    private final AtomicReference<State> state;
    private final ClientInfo owner;
    private final SessionRegister register;
    SessionRegistration(ClientInfo owner, ISessionDictClient.IKillListener killListener, SessionRegister register) {
        this.owner = owner;
        this.register = register;
        this.state = new AtomicReference<>(State.Registered);
        this.register.reg(owner, quit -> {
            if (log.isTraceEnabled()) {
                log.trace("Received quit request:reqId={},killer={}", quit.getReqId(), quit.getKiller());
            }
            killListener.onKill(quit.getKiller(), quit.getServerRedirection());
            state.set(State.Kicked);
            stop();
        });
        this.register.sendRegInfo(owner, true);
    }

    @Override
    public void stop() {
        if (state.get() == State.Quit) {
            return;
        }
        if (state.get() == State.Registered) {
            this.register.sendRegInfo(owner, false);
        }
        register.unreg(owner);
        state.set(State.Quit);
    }

    private enum State {
        Registered, Kicked, Quit
    }
}
