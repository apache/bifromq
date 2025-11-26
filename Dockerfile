# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

FROM debian@sha256:b4aa902587c2e61ce789849cb54c332b0400fe27b1ee33af4669e1f7e7c3e22f AS verifier

ARG TARGETARCH
ARG BIFROMQ_VERSION

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates tar \
    && rm -rf /var/lib/apt/lists/*

COPY apache-bifromq-${BIFROMQ_VERSION}.tar.gz /tmp/release/
COPY apache-bifromq-${BIFROMQ_VERSION}.tar.gz.sha512 /tmp/release/

RUN cd /tmp/release \
    && echo "$(awk '{print $1}' apache-bifromq-*.tar.gz.sha512)  apache-bifromq-${BIFROMQ_VERSION}.tar.gz" | sha512sum -c - \
    && mkdir /bifromq \
    && tar -zxvf apache-bifromq-*.tar.gz --strip-components 1 -C /bifromq

FROM debian@sha256:b4aa902587c2e61ce789849cb54c332b0400fe27b1ee33af4669e1f7e7c3e22f

ARG TARGETARCH

RUN groupadd -r -g 1000 bifromq \
    && useradd -r -m -u 1000 -g bifromq bifromq \
    && apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates net-tools lsof netcat-openbsd procps less openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-$TARGETARCH
ENV PATH=$JAVA_HOME/bin:$PATH

COPY --chown=bifromq:bifromq --from=verifier /bifromq /home/bifromq/

WORKDIR /home/bifromq

USER bifromq

EXPOSE 1883 1884 80 443

CMD ["./bin/standalone.sh", "start", "-fg"]
