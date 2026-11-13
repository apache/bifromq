<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements. See the NOTICE file
distributed with this work for additional information
regarding copyright ownership. The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied. See the License for the
specific language governing permissions and limitations
under the License.
-->

# Security Policy

Apache BifroMQ follows the [Apache Software Foundation vulnerability handling process](https://www.apache.org/security/).

## Reporting a vulnerability

Report suspected vulnerabilities privately through the [ASF security reporting process](https://security.apache.org/report/).
Do not publish vulnerability details in a GitHub issue, discussion, or public mailing list before coordinated disclosure.

Include the affected version, deployment configuration, the attacker's existing access, the trust boundary crossed,
the resulting impact, and a minimal proof of concept.

## Security model

The [Security Model](https://bifromq.apache.org/docs/admin_guide/security/intro/) describes the deployment assumptions,
trust boundaries, and responsibilities of BifroMQ and its operators. Consult the documentation for the version you run.
The following points summarize those boundaries:

* **MQTT clients are untrusted.** BifroMQ must safely process MQTT input, enforce the configured AuthProvider's
  authentication and authorization decisions, and preserve tenant separation after a client identity is established.
  Production deployments require a reviewed production AuthProvider. DevOnly behavior, the quick-start configuration,
  and the bundled DemoPlugin do not provide a production security baseline.
* **The HTTP API Server is a trusted control-plane interface.** It is enabled by default on port `8091` and does not
  authenticate or authorize HTTP callers at the application layer. Keep it on a trusted network or behind an API gateway
  that provides authentication and authorization. TLS, including mTLS, does not provide application-level tenant
  authorization. A request's `tenant_id` identifies the target tenant; it is not an authorization credential.
* **Cluster membership and RPC endpoints are for trusted nodes.** Operators must isolate cluster ports, control
  discovery, and configure transport security as needed. The cluster environment name provides logical grouping,
  not node authentication.
* **Plugins run as trusted code inside the BifroMQ process.** ClassLoader isolation separates dependencies; it is not
  a security sandbox. Operators must review plugins and protect their artifacts, credentials, and network access.
* **Operators control trusted deployment inputs.** Protect configuration files, JVM options, environment variables,
  certificates, DNS, stored data, and operational endpoints. Expose only the required MQTT listeners to untrusted clients
  and configure TLS, resource policies, and perimeter rate limits for the workload.

When using Proxy Protocol or forwarded client-address headers, restrict backend listener access to trusted load
balancers and ensure they replace client-supplied address metadata. Forwarded addresses are network metadata,
not authenticated client identities.

## Assessing security reports

Bypassing a correctly configured AuthProvider, unauthorized cross-tenant access, bypassing configured TLS or certificate
validation, or malformed or low-volume input causing unintended code execution, data disclosure, persistent corruption,
or disproportionate resource exhaustion may constitute a BifroMQ vulnerability.

An operator or trusted plugin exercising its existing privileges does not, by itself, demonstrate a crossed trust
boundary. Neither does exposing the API Server without the required network or gateway protection, or using DevOnly
and demonstration components as production security controls. These cases can still warrant documentation improvements
or hardening. Each report is assessed on its facts and on whether an untrusted actor crosses a boundary BifroMQ is
responsible for enforcing.
