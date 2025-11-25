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

ARG BASE_IMAGE=debian:bookworm-slim

FROM ${BASE_IMAGE} AS verifier

ARG TARGETARCH
ARG BIFROMQ_VERSION

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates gnupg dirmngr tar \
    && rm -rf /var/lib/apt/lists/*

COPY KEYS /tmp/release/KEYS
COPY apache-bifromq-${BIFROMQ_VERSION}.tar.gz /tmp/release/
COPY apache-bifromq-${BIFROMQ_VERSION}.tar.gz.asc /tmp/release/
COPY apache-bifromq-${BIFROMQ_VERSION}.tar.gz.sha512 /tmp/release/

RUN cd /tmp/release \
    && echo "$(awk '{print $1}' apache-bifromq-*.tar.gz.sha512)  apache-bifromq-${BIFROMQ_VERSION}.tar.gz" | sha512sum -c - \
    && gpg --import KEYS \
    && gpg --batch --verify apache-bifromq-*.tar.gz.asc apache-bifromq-*.tar.gz \
    && mkdir /bifromq \
    && tar -zxvf apache-bifromq-*.tar.gz --strip-components 1 -C /bifromq

FROM ${BASE_IMAGE}

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

# Set common command aliases
RUN echo "alias ll='ls -al'" >> ~/.bashrc

EXPOSE 1883 1884 80 443

CMD ["./bin/standalone.sh", "start", "-fg"]
