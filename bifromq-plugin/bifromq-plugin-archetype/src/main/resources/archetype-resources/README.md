# BifroMQ Plugin Archetype

This Maven Archetype helps you quickly bootstrap a new BifroMQ plugin project with a pre-configured project structure and necessary dependencies.

## Prerequisites

Before you begin, ensure you have the following installed on your system:

- Java JDK 17 or higher
- Maven 3.6.3 or higher

## Generating a Project

To generate a new project using the BifroMQ Plugin Archetype, run the following command in your terminal:

```bash
mvn archetype:generate \
    -DarchetypeGroupId=org.apache.bifromq \
    -DarchetypeArtifactId=bifromq-plugin-archetype \
    -DarchetypeVersion=${bifromqVersion} \
    -DgroupId=com.yourcompany.newproject \
    -DartifactId=your-plugin-name \
    -Dversion=1.0.0-SNAPSHOT \
    -DpluginName=YourPluginClassName \
    -DpluginContextName=YourPluginContextClassName \
    -DbifromqVersion=BifroMQVersion \
    -DinteractiveMode=false
```

Replace com.yourcompany.newproject, your-plugin-name, YourPluginClassName, YourPluginContextClassName, BifroMQVersion with your own values.

## The structure of the generated project

The generated project is a multi-module Maven project with the following structure:
```plaintext
your-plugin-name/
├── auth-provider/ <-- auth provider module as a reference for other bifromq plugin, you can remove it if not needed
│   └── src/
│       └── main/
│           └── java/
│               └── com.yourcompany.newproject/
│                   └── YourPluginClassNameAuthProvider.java
├── plugin-build/  <-- plugin-build module to build the plugin zip file
│   ├── assembly/
│   │   └── assembly-zip.xml
│   ├── conf/      <-- folder to contain plugin configuration files
│   │   ├── config.yaml <-- plugin configuration file
│   │   └── log4j2.xml  <-- log4j configuration file for the plugin
│   ├── src/
│   │   └── main/
│   │       └── java/
│   │           └──com.yourcompany.newproject/
│   │               └── YourPluginClassName.java <-- Your plugin main class
│   └── target/
│       └── pom.xml
├── plugin-context/  <-- plugin-context module to define the plugin context
│   └── src/
│       └── main/
│           └── java/
│               └── ─com.yourcompany.newproject/
│                   └──YourPluginContextClassName.java
└── pom.xml
```

## Building the Project

```bash
mvn clean package
```

The output plugin zip file will be generated in the `target` directory. Install the plugin by copying the zip file into the BifroMQ plugin folder. Ensure you verify the plugin is loaded correctly by checking the BifroMQ management console or
logs.