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

package org.apache.bifromq.baserpc.metrics;

import org.apache.bifromq.baserpc.BluePrint;
import io.micrometer.core.instrument.Meter;

public enum RPCMetric {
    UnaryReqDepth("unary.depth.summary", Meter.Type.DISTRIBUTION_SUMMARY, BluePrint.MethodType.UNARY),
    UnaryReqSendCount("unary.req.send.count", Meter.Type.COUNTER, BluePrint.MethodType.UNARY),
    UnaryReqCompleteCount("unary.req.complete.count", Meter.Type.COUNTER, BluePrint.MethodType.UNARY),
    UnaryReqAbortCount("unary.req.abort.count", Meter.Type.COUNTER, BluePrint.MethodType.UNARY),
    UnaryReqDropCount("unary.req.drop.count", Meter.Type.COUNTER, BluePrint.MethodType.UNARY),
    UnaryReqLatency("unary.req.finish.time", Meter.Type.TIMER, BluePrint.MethodType.UNARY),

    UnaryReqReceivedCount("unary.req.recv.count", Meter.Type.COUNTER, BluePrint.MethodType.UNARY),
    UnaryReqFulfillCount("unary.req.fulfil.count", Meter.Type.COUNTER, BluePrint.MethodType.UNARY),
    UnaryReqFailCount("unary.req.fail.count", Meter.Type.COUNTER, BluePrint.MethodType.UNARY),
    UnaryReqProcessLatency("unary.req.process.time", Meter.Type.TIMER, BluePrint.MethodType.UNARY),

    ReqPipelineCreateCount("ppln.create.count", Meter.Type.COUNTER, BluePrint.MethodType.PIPELINE_UNARY),
    ReqPipelineErrorCount("ppln.error.count", Meter.Type.COUNTER, BluePrint.MethodType.PIPELINE_UNARY),
    ReqPipelineCompleteCount("ppln.complete.count", Meter.Type.COUNTER, BluePrint.MethodType.PIPELINE_UNARY),


    ReqPipelineDepth("ppln.depth.summary", Meter.Type.DISTRIBUTION_SUMMARY, BluePrint.MethodType.PIPELINE_UNARY),
    PipelineReqAcceptCount("ppln.req.enqueue.count", Meter.Type.COUNTER, BluePrint.MethodType.PIPELINE_UNARY),
    PipelineReqSendCount("ppln.req.send.count", Meter.Type.COUNTER, BluePrint.MethodType.PIPELINE_UNARY),
    PipelineReqCompleteCount("ppln.req.complete.count", Meter.Type.COUNTER, BluePrint.MethodType.PIPELINE_UNARY),
    PipelineReqAbortCount("ppln.req.abort.count", Meter.Type.COUNTER, BluePrint.MethodType.PIPELINE_UNARY),
    PipelineReqDropCount("ppln.req.drop.count", Meter.Type.COUNTER, BluePrint.MethodType.PIPELINE_UNARY),
    PipelineReqQueueTime("ppln.req.queue.time", Meter.Type.TIMER, BluePrint.MethodType.PIPELINE_UNARY),
    PipelineReqLatency("ppln.req.finish.time", Meter.Type.TIMER, BluePrint.MethodType.PIPELINE_UNARY),

    PipelineReqReceivedCount("ppln.req.recv.count", Meter.Type.COUNTER, BluePrint.MethodType.PIPELINE_UNARY),
    PipelineReqFulfillCount("ppln.req.fulfil.count", Meter.Type.COUNTER, BluePrint.MethodType.PIPELINE_UNARY),
    PipelineReqFailCount("ppln.req.fail.count", Meter.Type.COUNTER, BluePrint.MethodType.PIPELINE_UNARY),
    PipelineReqProcessTime("ppln.req.process.time", Meter.Type.TIMER, BluePrint.MethodType.PIPELINE_UNARY),

    StreamCreateCount("stream.create.count", Meter.Type.COUNTER, BluePrint.MethodType.STREAMING),
    StreamCompleteCount("stream.complete.count", Meter.Type.COUNTER, BluePrint.MethodType.STREAMING),
    StreamErrorCount("stream.error.count", Meter.Type.COUNTER, BluePrint.MethodType.STREAMING),

    StreamAckAcceptCount("stream.ack.enqueue.count", Meter.Type.COUNTER, BluePrint.MethodType.STREAMING),
    StreamAckDropCount("stream.ack.drop.count", Meter.Type.COUNTER, BluePrint.MethodType.STREAMING),
    StreamAckSendCount("stream.ack.send.count", Meter.Type.COUNTER, BluePrint.MethodType.STREAMING),
    StreamMsgReceiveCount("stream.msg.recv.count", Meter.Type.COUNTER, BluePrint.MethodType.STREAMING),

    StreamMsgSendCount("stream.msg.send.count", Meter.Type.COUNTER, BluePrint.MethodType.STREAMING),
    StreamAckReceiveCount("stream.ack.recv.count", Meter.Type.COUNTER, BluePrint.MethodType.STREAMING);

    public final String metricName;
    public final Meter.Type meterType;
    public final BluePrint.MethodType methodType;

    RPCMetric(String metricName, Meter.Type meterType, BluePrint.MethodType methodType) {
        this.metricName = metricName;
        this.meterType = meterType;
        this.methodType = methodType;
    }
}
