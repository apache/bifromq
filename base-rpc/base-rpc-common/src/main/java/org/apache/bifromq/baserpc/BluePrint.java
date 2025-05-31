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

package org.apache.bifromq.baserpc;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static java.util.Collections.unmodifiableMap;

import io.grpc.MethodDescriptor;
import io.grpc.ServiceDescriptor;
import io.grpc.protobuf.lite.EnhancedMarshaller;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.NoArgsConstructor;

/**
 * BluePrint is a configuration class for a service. It contains the service descriptor, method semantics, and method
 */
public final class BluePrint {
    private final ServiceDescriptor serviceDescriptor;
    private final Map<String, MethodSemantic> methodSemantics;
    private final Map<String, MethodDescriptor<?, ?>> methods;
    private final Map<String, MethodDescriptor<?, ?>> wrappedMethods;

    private BluePrint(
        ServiceDescriptor serviceDescriptor,
        Map<String, MethodSemantic> methodSemantics,
        Map<String, MethodDescriptor<?, ?>> methods,
        Map<String, MethodDescriptor<?, ?>> wrappedMethods) {
        this.serviceDescriptor = serviceDescriptor;
        this.methodSemantics = methodSemantics;
        this.methods = methods;
        this.wrappedMethods = wrappedMethods;
        if (!serviceDescriptor.getMethods().containsAll(methods.values())) {
            throw new RuntimeException("Some method is not defined in the supplied service descriptor");
        }
        for (String methodName : methodSemantics.keySet()) {
            MethodDescriptor<?, ?> methodDesc = wrappedMethods.get(methodName);
            MethodSemantic semantic = methodSemantics.get(methodName);
            switch (methodDesc.getType()) {
                case UNARY:
                    if (!(semantic instanceof Unary)) {
                        // unary rpc could not be configured as pipelining method
                        throw new RuntimeException("Wrong semantic for Unary rpc");
                    }
                    break;
                case BIDI_STREAMING:
                    if (!(semantic instanceof PipelineUnary) && !(semantic instanceof Streaming)) {
                        // bidi streaming rpc could only be configured as either request/response pipeline
                        // or wrr/wch streaming method
                        throw new RuntimeException("Wrong semantic configured for bidi streaming rpc");
                    }
                    break;
                default:
                    throw new RuntimeException("Unknown method type: " + methodDesc.getType());
            }
        }
    }

    public static BluePrintBuilder builder() {
        return new BluePrintBuilder();
    }

    public ServiceDescriptor serviceDescriptor() {
        return serviceDescriptor;
    }

    public Set<String> allMethods() {
        return wrappedMethods.keySet();
    }

    public MethodSemantic semantic(String fullMethodName) {
        return methodSemantics.get(fullMethodName);
    }

    @SuppressWarnings("unchecked")
    public <ReqT, RespT> MethodDescriptor<ReqT, RespT> methodDesc(String fullMethodName) {
        return (MethodDescriptor<ReqT, RespT>) wrappedMethods.get(fullMethodName);
    }

    /**
     * The rpc type of method.
     */
    public enum MethodType {
        UNARY,
        PIPELINE_UNARY,
        STREAMING,
    }

    /**
     * THe balance mod of method.
     */
    public enum BalanceMode {
        DDBalanced,
        WRRBalanced,
        WRBalanced,
        WCHBalanced,
    }

    public interface MethodSemantic {
        MethodType type();

        BalanceMode mode();
    }

    public interface Unary extends MethodSemantic {
        default MethodType type() {
            return MethodType.UNARY;
        }
    }

    public interface PipelineUnary extends MethodSemantic {
        default MethodType type() {
            return MethodType.PIPELINE_UNARY;
        }
    }

    public interface Streaming extends MethodSemantic {
        default MethodType type() {
            return MethodType.STREAMING;
        }
    }

    public interface WCHBalancedReq<ReqT> {
        // marker interface
        String hashKey(ReqT req);
    }

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static final class DDUnaryMethod implements Unary {
        public static DDUnaryMethod getInstance() {
            return new DDUnaryMethod();
        }

        @Override
        public BalanceMode mode() {
            return BalanceMode.DDBalanced;
        }
    }

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static final class WRRUnaryMethod implements Unary {
        public static WRRUnaryMethod getInstance() {
            return new WRRUnaryMethod();
        }

        @Override
        public BalanceMode mode() {
            return BalanceMode.WRRBalanced;
        }
    }

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static final class WRUnaryMethod implements Unary {
        public static WRUnaryMethod getInstance() {
            return new WRUnaryMethod();
        }

        @Override
        public BalanceMode mode() {
            return BalanceMode.WRBalanced;
        }
    }

    @Builder
    public static final class WCHUnaryMethod<ReqT> implements Unary, WCHBalancedReq<ReqT> {
        private final Function<ReqT, String> keyHashFunc;

        public String hashKey(ReqT req) {
            return keyHashFunc.apply(req);
        }

        @Override
        public BalanceMode mode() {
            return BalanceMode.WCHBalanced;
        }
    }

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static final class DDPipelineUnaryMethod implements PipelineUnary {
        public static DDPipelineUnaryMethod getInstance() {
            return new DDPipelineUnaryMethod();
        }

        @Override
        public BalanceMode mode() {
            return BalanceMode.DDBalanced;
        }
    }

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static final class WRRPipelineUnaryMethod implements PipelineUnary {
        public static WRRPipelineUnaryMethod getInstance() {
            return new WRRPipelineUnaryMethod();
        }

        @Override
        public BalanceMode mode() {
            return BalanceMode.WRRBalanced;
        }
    }

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static final class WRPipelineUnaryMethod implements PipelineUnary {
        public static WRPipelineUnaryMethod getInstance() {
            return new WRPipelineUnaryMethod();
        }

        @Override
        public BalanceMode mode() {
            return BalanceMode.WRBalanced;
        }
    }

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static final class WCHPipelineUnaryMethod implements PipelineUnary {
        public static WCHPipelineUnaryMethod getInstance() {
            return new WCHPipelineUnaryMethod();
        }

        @Override
        public BalanceMode mode() {
            return BalanceMode.WCHBalanced;
        }
    }

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static final class DDStreamingMethod implements Streaming {
        public static DDStreamingMethod getInstance() {
            return new DDStreamingMethod();
        }

        @Override
        public BalanceMode mode() {
            return BalanceMode.DDBalanced;
        }
    }

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static final class WRRStreamingMethod implements Streaming {
        public static WRRStreamingMethod getInstance() {
            return new WRRStreamingMethod();
        }

        @Override
        public BalanceMode mode() {
            return BalanceMode.WRRBalanced;
        }
    }

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static final class WRStreamingMethod implements Streaming {
        public static WRStreamingMethod getInstance() {
            return new WRStreamingMethod();
        }

        @Override
        public BalanceMode mode() {
            return BalanceMode.WRBalanced;
        }
    }

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static final class WCHStreamingMethod implements Streaming {
        public static WCHStreamingMethod getInstance() {
            return new WCHStreamingMethod();
        }

        @Override
        public BalanceMode mode() {
            return BalanceMode.WCHBalanced;
        }
    }

    public static class BluePrintBuilder {
        private ServiceDescriptor serviceDescriptor;
        private ArrayList<MethodDescriptor<?, ?>> methods;
        private ArrayList<MethodDescriptor<?, ?>> wrappedMethods;
        private ArrayList<MethodSemantic> methodSemantics;

        BluePrintBuilder() {
        }

        public BluePrintBuilder serviceDescriptor(ServiceDescriptor serviceDescriptor) {
            this.serviceDescriptor = serviceDescriptor;
            return this;
        }

        public <ReqT, RespT> BluePrintBuilder methodSemantic(
            MethodDescriptor<ReqT, RespT> methodSemanticKey, MethodSemantic methodSemanticValue) {
            if (this.methods == null) {
                this.methods = new ArrayList<>();
                this.wrappedMethods = new ArrayList<>();
                this.methodSemantics = new ArrayList<>();
            }
            this.methodSemantics.add(methodSemanticValue);
            this.methods.add(methodSemanticKey);
            this.wrappedMethods.add(methodSemanticKey.toBuilder()
                .setRequestMarshaller(enhance((MethodDescriptor.PrototypeMarshaller<ReqT>)
                    methodSemanticKey.getRequestMarshaller()))
                .setResponseMarshaller(enhance((MethodDescriptor.PrototypeMarshaller<RespT>)
                    methodSemanticKey.getResponseMarshaller()))
                .build());
            return this;
        }

        public BluePrint build() {
            Map<String, MethodDescriptor<?, ?>> methodsMap;
            Map<String, MethodDescriptor<?, ?>> wrappedMethods;
            Map<String, MethodSemantic> methodSemanticMap;
            switch (this.wrappedMethods == null ? 0 : this.wrappedMethods.size()) {
                case 0:
                    methodSemanticMap = emptyMap();
                    methodsMap = emptyMap();
                    wrappedMethods = emptyMap();
                    break;
                case 1: {
                    MethodDescriptor<?, ?> method = this.methods.get(0);
                    String fullMethodName = method.getFullMethodName();
                    methodSemanticMap = singletonMap(fullMethodName, this.methodSemantics.get(0));
                    methodsMap = singletonMap(fullMethodName, method);
                    wrappedMethods = singletonMap(fullMethodName, this.wrappedMethods.get(0));
                }
                break;
                default:
                    methodSemanticMap =
                        new java.util.LinkedHashMap<>(
                            this.wrappedMethods.size() < 1073741824
                                ? 1 + this.wrappedMethods.size() + (this.wrappedMethods.size() - 3) / 3
                                : Integer.MAX_VALUE);
                    methodsMap =
                        new java.util.LinkedHashMap<>(
                            this.methods.size() < 1073741824
                                ? 1 + this.methods.size() + (this.methods.size() - 3) / 3
                                : Integer.MAX_VALUE);
                    wrappedMethods =
                        new java.util.LinkedHashMap<>(
                            this.wrappedMethods.size() < 1073741824
                                ? 1 + this.wrappedMethods.size() + (this.wrappedMethods.size() - 3) / 3
                                : Integer.MAX_VALUE);
                    for (int $i = 0; $i < this.methods.size(); $i++) {
                        MethodDescriptor<?, ?> method = this.methods.get($i);
                        String fullMethodName = method.getFullMethodName();
                        methodSemanticMap.put(fullMethodName, this.methodSemantics.get($i));
                        methodsMap.put(fullMethodName, method);
                        wrappedMethods.put(fullMethodName, this.wrappedMethods.get($i));
                    }
                    methodSemanticMap = unmodifiableMap(methodSemanticMap);
                    methodsMap = unmodifiableMap(methodsMap);
                    wrappedMethods = unmodifiableMap(wrappedMethods);
            }

            return new BluePrint(serviceDescriptor, methodSemanticMap, methodsMap, wrappedMethods);
        }

        private <T> MethodDescriptor.PrototypeMarshaller<T> enhance(
            MethodDescriptor.PrototypeMarshaller<T> marshaller) {
            return new EnhancedMarshaller<>(marshaller.getMessagePrototype());
        }
    }
}
