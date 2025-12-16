<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

You might wonder why the package is named under `io.grpc`. The reason is that we need to implement a special client
channel:
when the RPC client and server are in the same process, we use a lighter `InProcTransport` to improve the efficiency of
RPC within a single process. Unfortunately, the current code structure of gRPC Java only exposes limited public customization
capabilities.
In the long run, it is very likely that we will replace the usage of gRPC with an implementation that only meets our
specific RPC needs