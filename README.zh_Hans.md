# BifroMQ

[English](./README.md) | 中文简体

---

BifroMQ 是一个高性能且支持分布式的 MQTT Broker 实现，它原生支持多租户，专为构建大规模 IoT
设备连接与消息系统而设计。

## 特性

* 完整支持 MQTT 3.1/3.1.1/5.0 的所有特性，包括可通过TCP, TLS, WS, WSS的方式访问
* 原生支持多租户资源共享和工作负载隔离
* 内置存储引擎，针对关键负载定向优化，无第三方中间件依赖。
* 扩展机制支持：
  * 认证/授权 (Authentication/Authorization)
  * 租户级运行时设置 (Runtime Setting)
  * 租户级资源限制 (Resource Throttling)
  * 事件 (Event)
  * 系统/租户级别的监控指标 (System/Tenant-level Metrics)

## 文档

您可以在官方[网站](https://bifromq.apache.org)上查看[文档](https://bifromq.apache.org/docs/get_started/intro/)。
此外，欢迎您在GitHub[仓库](https://github.com/apache/bifromq-sites)中贡献文档。

## 开始使用

### Docker

```
docker run -d -m <MEM_LIMIT> -e MEM_LIMIT='<MEM_LIMIT_IN_BYTES>' --name bifromq -p 1883:1883 bifromq/bifromq:latest
```

将`<MEM_LIMIT>`和`<MEM_LIMIT_IN_BYTES>`替换为 Docker 进程的实际内存分配，例如，使用`2G`替换`<MEM_LIMIT>`，使用 `2147483648`
替换`<MEM_LIMIT_IN_BYTES>`。如果未指定这些值，BifroMQ 默认使用宿主服务器的物理内存来确定JVM参数。这可能导致 Docker
进程被宿主机的OOM Killer终止，更多供信息[参考](https://bifromq.apache.org/docs/installation/docker/)。

你可以使用Docker Compose在单个host上搭建BifroMQ集群用于开发和测试。假设你想创建一个包含三个节点的集群：node1、node2 和 node3，目录结构如下：
```
|- docker-compose.yml
|- node1
|- node2
|- node3
```
每个节点对应的配置文件如下：
```yml
clusterConfig:
  env: "Test"
  host: bifromq-node1 # 对于node2和node3分别改成bifromq-node2和bifromq-node3
  port: 8899
  seedEndpoints: "bifromq-node1:8899,bifromq-node2:8899,bifromq-node3:8899"
```
`docker-compose.yml` 文件定义了三个节点的服务：
```yml
services:
  bifromq-node1:
    image: bifromq/bifromq:latest
    container_name: bifromq-node1
    volumes:
      - ./node1/standalone.yml:/home/bifromq/conf/standalone.yml
    ports:
      - "1883:1883"
    environment:
      - MEM_LIMIT=2147483648 # 根据host的实际配置进行调整
    networks:
      - bifromq-net

  bifromq-node2:
    image: bifromq/bifromq:latest
    container_name: bifromq-node2
    volumes:
      - ./node2/standalone.yml:/home/bifromq/conf/standalone.yml
    ports:
      - "1884:1883"
    environment:
      - MEM_LIMIT=2147483648
    networks:
      - bifromq-net

  bifromq-node3:
    image: bifromq/bifromq:latest
    container_name: bifromq-node3
    volumes:
      - ./node3/standalone.yml:/home/bifromq/conf/standalone.yml
    ports:
      - "1885:1883"
    environment:
      - MEM_LIMIT=2147483648
    networks:
      - bifromq-net

networks:
  bifromq-net:
    driver: bridge
```
运行以下命令启动集群：
```shell
docker compose up -d
```

### 从源码构建

#### 预备条件

* JDK 17+
* Maven 3.5.0+

#### 获取源码并构建

将仓库克隆到您的本地工作空间：

```
cd <YOUR_WORKSPACE>
git clone https://github.com/apache/bifromq bifromq
```

进入项目文件夹，执行以下命令来构建整个项目：

```
cd bifromq
mvn wrapper:wrapper
./mvnw -U clean package
```

构建输出位于`/target/output`下：

* `bifromq-<VERSION>.tar.gz`
* `bifromq-<VERSION>-windows.zip`

#### 运行测试

在项目根文件夹执行以下命令来运行所有测试用例，包括单元测试和集成测试。
注意：完整的测试运行可能需要一些时间

```
mvn test
```

### 快速开始

要快速启动一个 BifroMQ 单机服务器，请先将 `bifromq-<VERSION>.tar.gz` 文件解压到某个目录中。解压后的目录结构如下所示：

```
|- bin
|- conf
|- lib
|- plugins
```

然后，在 `bin` 目录下执行以下命令：

- **启动服务器**：

  ```
  ./standalone.sh start // 这将会在后台启动服务进程
  ```

- **停止服务器**：

  ```
  ./standalone.sh stop
  ```

配置文件 `standalone.yml` 位于 `conf`
目录下。大多数配置项的名称都是自解释的，容易理解其功能。默认情况下，单机服务器会将持久化数据保存在 `data` 目录中。

### 插件开发

执行以下 Maven 命令，快速启动 BifroMQ 插件开发：

```bash
mvn archetype:generate \
    -DarchetypeGroupId=org.apache.bifromq \
    -DarchetypeArtifactId=bifromq-plugin-archetype \
    -DarchetypeVersion=<BIFROMQ_VERSION> \
    -DgroupId=<YOUR_GROUP_ID> \
    -DartifactId=<YOUR_ARTIFACT_ID> \
    -Dversion=<YOUR_PROJECT_VERSION> \
    -DpluginName=<YOUR_PLUGIN_CLASS_NAME> \
    -DpluginContextName=<YOUR_PLUGIN_CONTEXT_CLASS_NAME> \
    -DbifromqVersion=<BIFROMQ_VERSION> \
    -DinteractiveMode=false
```

请将 `<YOUR_GROUP_ID>`、`<YOUR_ARTIFACT_ID>`、`<YOUR_PROJECT_VERSION>`、`<YOUR_PLUGIN_CLASS_NAME>`
和 `<YOUR_PLUGIN_CONTEXT_CLASS_NAME>` 替换为您的具体信息。该命令生成一个准备就绪的、结构清晰的多模块项目，专为 BifroMQ
插件开发而设计。

**重要提示：**原型的版本应为 3.2.0 或更高版本，因为该原型从 3.2.0 版本开始兼容。确保 `<BIFROMQ_VERSION>` 设置正确。

### 集群部署

BifroMQ支持两种集群部署模式：`标准集群(Standard Cluster)`，`独立工作负载集群(Independent-Workload Cluster)`

#### 标准集群

标准集群部署模式适用于需要可靠性和可扩展性的小型到中型生产环境。 它由几个完全功能的Standalone节点组成，共同作为一个逻辑
MQTT Broker 实例，确保高可用性。 你也可以通过添加更多的节点来扩大并发 MQTT 连接的工作负载，而在这种模式下，某些类型的消息相关的工作负载并不是水平可扩展的。

#### 独立工作负载集群

独立工作负载集群部署模式旨在构建大规模的，多租户的 Serverless
集群。在这种模式下，集群由几个专门的子集群组成，每个子集群都专注于一个特定的'独立类型'的工作负载。这些子集群共同协作形成一个逻辑的
MQTT Broker 实例。这是最复杂的部署模式，需要额外的非开源协作组件。

## 用户社区

欢迎加入 BifroMQ 社区，与我们保持联系：

* **[邮件列表](https://bifromq.apache.org/community/#about-the-mailing-list)** – 通过公开的邮件列表获取最新信息、参与开发话题讨论，并与其他贡献者协作。
* **[Discord](https://discord.gg/Pfs3QRadRB)** – 加入我们，进行实时聊天、分享想法，并获取项目的最新动态。

## ASF Incubator disclaimer

Apache BifroMQ&trade; is an effort undergoing incubation at The Apache Software Foundation (ASF), sponsored by the Apache Incubator. Incubation is required of all newly accepted projects until a further review indicates that the infrastructure, communications, and decision making process have stabilized in a manner consistent with other successful ASF projects. While incubation status is not necessarily a reflection of the completeness or stability of the code, it does indicate that the project has yet to be fully endorsed by the ASF.
