# BifroMQ

[![GitHub Release](https://img.shields.io/github/release/bifromqio/bifromq?color=brightgreen&label=Release)](https://github.com/bifromqio/bifromq/releases)
<a href="https://discord.gg/Pfs3QRadRB"><img src="https://img.shields.io/discord/1115542029531885599?logo=discord&logoColor=white" alt="BifroMQ Discord server" /></a>
[![Coverage Status](https://img.shields.io/coveralls/github/bifromqio/bifromq/main?label=Coverage)](https://coveralls.io/github/bifromqio/bifromq?branch=main)
[![Docker Pulls](https://img.shields.io/docker/pulls/bifromq/bifromq?label=Docker%20Pulls)](https://hub.docker.com/r/bifromq/bifromq)

English | [中文简体](./README.zh_Hans.md)

---

BifroMQ is a high-performance, distributed MQTT broker implementation that seamlessly integrates native multi-tenancy
support. It is designed to support building large-scale IoT device connections and messaging systems.

## Features

* Full support for MQTT 3.1, 3.1.1 and 5.0 features over TCP, TLS, WS, WSS
* Native support for multi-tenancy resource sharing and workload isolation
* Built-in distributed storage engine optimized for MQTT workloads, with no third-party middleware dependencies.
* Extension mechanism for supporting:
  * Authentication/Authorization
  * Tenant-level Runtime Setting
  * Tenant-level Resource Throttling
  * Event
  * System/Tenant-level Metrics

## Documentation

You can access the [documentation](https://bifromq.apache.org/docs/get_started/intro/) on the
official [website](https://bifromq.apache.org).
Additionally, contributions to the documentation are welcome in the
GitHub [repository](https://github.com/apache/bifromq-sites).

## Getting Started

### Docker

```
docker run -d -m <MEM_LIMIT> -e MEM_LIMIT='<MEM_LIMIT_IN_BYTES>' --name bifromq -p 1883:1883 bifromq/bifromq:latest
```

Substitute `<MEM_LIMIT>` and `<MEM_LIMIT_IN_BYTES>` with the actual memory allocation for the Docker process, for
example, `2G` for `<MEM_LIMIT>` and `2147483648` for `<MEM_LIMIT_IN_BYTES>`. If not specified, BifroMQ defaults to using
the hosting server's physical memory for determining JVM parameters. This can result in the Docker process being
terminated by the host's Out-of-Memory (OOM) Killer. Refer to [here](https://bifromq.apache.org/docs/installation/docker/)
for more information.

You can build a BifroMQ cluster using Docker Compose on a single host for development and testing. Suppose you want to create a cluster with three nodes: node1, 
node2, and node3. The directory structure should be as follows:
```
|- docker-compose.yml
|- node1
|- node2
|- node3
```
Each node should have a configuration file, it is defined as follows:
```yml
clusterConfig:
  env: "Test"
  host: bifromq-node1 # Change this to bifromq-node2 for node2 and bifromq-node3 for node3
  port: 8899
  seedEndpoints: "bifromq-node1:8899,bifromq-node2:8899,bifromq-node3:8899"
```
The `docker-compose.yml` file defines the services for the three nodes:
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
      - MEM_LIMIT=2147483648 # Adjust the value according to the actual host configuration.
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
To launch the cluster, run the following command:
```shell
docker compose up -d
```
### Build from source

#### Prerequisites

* JDK 17+
* Maven 3.5.0+

#### Get source & Build

Clone the repository to your local workspace:

```
cd <YOUR_WORKSPACE>
git clone https://github.com/apache/bifromq bifromq
```

Navigate to the project root folder and execute the following commands to build the entire project:

```
cd bifromq
mvn wrapper:wrapper
./mvnw -U clean package
```

The build output consists of several archive files located under `/target/output`

* `bifromq-<VERSION>.tar.gz`
* `bifromq-<VERSION>-windows.zip`

#### Running the tests

Execute the following command in the project root folder to run all test cases, including unit tests and integration
tests.
Note: The tests may take some time to finish

```
mvn test
```

### Quick Start

To quickly set up a BifroMQ server, extract the `bifromq-<VERSION>.tar.gz` file into a directory. You will see the
following directory structure:

```
|- bin
|- conf
|- lib
|- plugins
```

To start or stop the server, execute the respective command in the `bin` directory:

- **To start the server**, run:
  ```
  ./standalone.sh start // This starts the server process in the background.
  ```

- **To stop the server**, run:
  ```
  ./standalone.sh stop
  ```

The configuration file, `standalone.yml`, can be found in the `conf` directory. The settings within this file are named
in a self-explanatory manner. By default, the standalone server stores persistent data in the `data` directory.

### Plugin Development

To jump start your BifroMQ plugin development, execute the following Maven command:

```
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

Replace `<YOUR_GROUP_ID>`, `<YOUR_ARTIFACT_ID>`, `<YOUR_PROJECT_VERSION>`, `<YOUR_PLUGIN_CLASS_NAME>`,
and `< YOUR_PLUGIN_CONTEXT_CLASS_NAME>` with your specific details. This command generates a ready-to-build multi-module
project structured for BifroMQ plugin development.

Important Note: The archetype version should be 3.2.0 or higher as the archetype is compatible starting from version
3.2.0. Ensure that <BIFROMQ_VERSION> is set accordingly.

### Cluster Deployment

BifroMQ has two cluster deployment modes: `Standard Cluster`, `Independent-Workload Cluster`

#### Standard Cluster

The standard cluster deployment mode is suitable for small to medium-sized production environments that require
reliability and scalability. It comprises several fully functional standalone nodes working together as a logical MQTT
broker
instance, ensuring high availability. You can also scale up the concurrent mqtt connection workload by adding more
nodes, while some types of messaging related workload are not horizontal scalable in this mode.

#### Independent Workload Cluster

The Independent Workload Cluster deployment mode is designed for building large-scale, multi-tenant serverless clusters.
In this mode, the cluster consists of several specialized sub-clusters, each focusing on a particular 'independent type'
of workload. These sub-clusters work together coherently to form a logical MQTT broker instance. This is the most
complex deployment mode and requires additional non-open-sourced building blocks. Feel free to contact us for commercial
support.

## User Community

Join our [Discord](https://discord.gg/Pfs3QRadRB) if you are interested in our work.

## ASF Incubator disclaimer

Apache BifroMQ&trade; is an effort undergoing incubation at The Apache Software Foundation (ASF), sponsored by the Apache Incubator. Incubation is required of all newly accepted projects until a further review indicates that the infrastructure, communications, and decision making process have stabilized in a manner consistent with other successful ASF projects. While incubation status is not necessarily a reflection of the completeness or stability of the code, it does indicate that the project has yet to be fully endorsed by the ASF.
 