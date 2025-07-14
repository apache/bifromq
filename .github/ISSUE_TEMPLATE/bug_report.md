---
name: Bug report
about: Create a report to help us improve
title: ''
labels: ''
assignees: ''

---

## ⚠️ *Important: Please read before submitting an issue*

To ensure prompt and effective support from our community, please provide one of the following with your issue
description:

1. Complete and reproducible steps to consistently recreate the problem, including:

- The version of BifroMQ being used (Note: Unless otherwise stated, the BifroMQ team only supports the latest major
  version. Please try to reproduce the issue with the latest version)
- Detailed deployment configuration **without custom plugins**
- Hardware specifications, OS and Networking setup
- Any publicly available test tools or commands (e.g., `eMQTT-Bench`, `Mosquitto_pub/sub`, `HiveMQ mqtt cli`) 
  used to reproduce the issue
- OR source code of a custom reproduction program. The source code must be complete, directly compilable, and 
  executable. Issues with incomplete reproduction source code will not be accepted.

OR

2. All relevant data or environmental details that can aid in diagnosing the issue, made publicly accessible (e.g., via
   public file sharing or accessible links). This may include but is not limited to:

- All files under `logs` folder with **Debug-Level** output (mandatory)
- All contents of the `data` directory for BifroMQ（mandatory if your issue related to persistent workflow）
- The content of configuration files and other relevant information
- Any other pertinent details

**Issues that only describe symptoms without providing either of the above may not contain enough valuable data for
problem identification and are unlikely to receive a response from the team.**

We encourage you to help troubleshooting and submit a Pull Request with an Issue Fix to become a 
[contributor](https://bifromq.apache.org/docs/contribution_guide/intro/) to BifroMQ.

## ⚠️ *重要提示：提交问题前请仔细阅读*

为了确保社区能够迅速有效地为您提供支持，请在描述问题时提供以下两项中的任意一项：

1. 完整且可稳定复现问题的详细步骤，包括：

- 使用的 BifroMQ 版本（注意：除非特别声明，BifroMQ 团队仅对最新的主要版本提供支持。请尝试使用最新版本复现该问题）
- 详细的不包含**自定义插件**的部署配置
- 硬件规格和操作系统环境，网络环境
- 用于复现问题的公开可用测试工具命令（如：`eMQTT-Bench`, `Mosquitto_pub/sub`, `HiveMQ mqtt cli`)
- 或自定义复现程序源码。源码必须完整、可直接编译并运行，不接受复现程序源码不完整的Issue

或者

2. 有助于诊断问题的所有相关数据或环境细节，以公开可访问的方式提供（例如，通过公共文件共享或可访问的链接）。这可能包括但不限于：

- **调试**级别下的`logs`目录下的全部日志（必须）
- 完整的BifroMQ `data` 目录的内容（必须，如果问题设计持久化相关的负载）
- 配置文件内容和其他相关信息
- 其他任何相关的重要信息

**仅描述现象而不提供上述两项中任意一项的问题报告，通常无法为问题定位提供足够有价值的信息，因此可能不会得到团队的响应。**

我们鼓励您能帮助定位问题，提交解决问题的 Pull Request，成为 BifroMQ 的[贡献者](https://bifromq.apache.org/docs/contribution_guide/intro/)。

### **Describe the bug**

A clear and concise description of what the bug is, and what you expected to happen.

#### **Environment**

- Version: [e.g. 3.2.x **We highly recommend you to verify your issue using the latest version first.**]
- JVM Version: [e.g. OpenJDK17]
- Hardware Spec: [e.g. 4C16G, 1Gbps NIC]
- OS: [e.g. CentOS 8]
- The Testing Tools
- etc

#### **Reproducible Steps**

Steps to reproduce the behavior

1. The steps to setup your cluster, including the configuration for each node
2. The testing steps to reproduce the issue, including following important information:

- ***PUB Client Parameters***
  - MQTT Connection:
    - ClientIdentifier:
    - etc...
  - MQTT Pub:
    - Topic:
    - QoS: [0|1|2]
    - Retain: [true | false]
- ***SUB Client Parameters***:
  - MQTT Connection:
    - Clean Session: [true | false]
    - ClientIdentifier:
    - etc...
  - MQTT Sub:
    - TopicFilter:
    - QoS: [0|1|2]
- ***The Load Generated***
  - Connection Count:
    - PUB Client count: [e.g. 1000]
    - SUB Client count: [e.g. 1000]
  - Load Generated:
    - PUB QPS per connection: [e.g. 10msg/s]
    - SUB QPS per connection: [e.g. 10msg/s]
    - Payload size: [e.g. 1KB]
    - FanIn: [e.g. 5 means one sub client receives messages from average 5 pub clients]
    - FanOut: [e.g. 5 means one message is subscribed by average 5 sub clients]
    - topic and subscription
      pattern: [e.g. 10 pub clients send messages to the topic: tp/{deviceName}/event, which each client use its unique deviceName. One sub client subscribe the topicfilter tp/+/event to recieve all the messages.]

#### **Publicly Accessible Diagnostic Data**(If Reproducible Steps are not available)

- Log files downloadable link:
- BifroMQ `data` downloadable link:
- Configuration files downloadable link:
- etc

**Additional context**
Add any other context about the problem here.
