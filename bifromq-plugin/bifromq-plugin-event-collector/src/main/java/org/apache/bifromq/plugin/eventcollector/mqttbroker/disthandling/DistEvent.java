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

package org.apache.bifromq.plugin.eventcollector.mqttbroker.disthandling;

import org.apache.bifromq.plugin.eventcollector.ClientEvent;
import lombok.ToString;

@ToString(callSuper = true)
public abstract class DistEvent<T extends DistEvent<T>> extends ClientEvent<T> {
    private long reqId;

    private String topic;

    private int size;

    public final long reqId() {
        return this.reqId;
    }

    public final T reqId(long reqId) {
        this.reqId = reqId;
        return (T) this;
    }

    public final String topic() {
        return this.topic;
    }

    public final T topic(String topic) {
        this.topic = topic;
        return (T) this;
    }

    public final int size() {
        return size;
    }

    public final T size(int size) {
        this.size = size;
        return (T) this;
    }

    @Override
    public void clone(T orig) {
        super.clone(orig);
        this.size = orig.size();
        this.reqId = orig.reqId();
        this.topic = orig.topic();
    }
}
