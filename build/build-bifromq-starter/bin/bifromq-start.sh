#! /bin/bash

#
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
#

if [ $# -lt 4 ]; then
  echo "USAGE: $0 -c classname -f filename [-fg]"
  exit 1
fi

while [ $# -gt 0 ]; do
  COMMAND=$1
  case $COMMAND in
  -fg)
    FOREGROUND_MODE='true'
    shift 1
    ;;
  -c)
    NAME=$2
    shift 2
    ;;
  -f)
    FILE_NAME=$2
    shift 2
    ;;
  *)
    break
    ;;
  esac
done

BASE_DIR=$(
  cd "$(dirname "$0")"
  pwd
)/..

BIN_DIR="$BASE_DIR/bin"
CONF_DIR="$BASE_DIR/conf"
CONF_FILE="$CONF_DIR/$FILE_NAME"
PLUGIN_DIR="$BASE_DIR/plugins"
LOG_CONF_FILE="$CONF_DIR/log4j2.xml"
LIB_DIR="$BASE_DIR/lib"
CLASSPATH=$(echo "$LIB_DIR/*")

# Log directory to use
if [ "x$LOG_DIR" = "x" ]; then
  LOG_DIR="$BASE_DIR/logs"
fi
mkdir -p "$LOG_DIR"

# Data directory to use
if [ "x$DATA_DIR" = "x" ]; then
  DATA_DIR="$BASE_DIR/data"
fi
mkdir -p "$DATA_DIR"

PID_FILE="$BIN_DIR/pid"

pid() {
  if [ -f "$PID_FILE" ]; then
    cat "$PID_FILE"
  else
    echo ""
  fi
}

is_pid_running() {
  local pid=$1
  if [ -n "$pid" ] && ps -p "$pid" > /dev/null 2>&1; then
    return 0
  else
    return 1
  fi
}

total_memory() {
  if [ -n "$MEM_LIMIT" ]; then
    echo $MEM_LIMIT
  elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
    local total_kb=$(cat /proc/meminfo | grep 'MemTotal' | awk -F : '{print $2}' | awk '{print $1}' | sed 's/^[ \t]*//g')
    echo "$((total_kb * 1024))"
  elif [[ "$OSTYPE" == "darwin"* ]]; then
    echo "$(sysctl hw.memsize | awk -F : '{print $2}' | xargs)"
  else
    echo -1
  fi
}

memory_in_mb() {
  echo $(($1 / 1024 / 1024))
}

existing_pid=$(pid)
if is_pid_running "$existing_pid"; then
  echo "$NAME is already running with PID $existing_pid"
  exit 0
fi

# get java version
if [ -z "$JAVA_HOME" ]; then
  JAVA="java"
else
  JAVA="$JAVA_HOME/bin/java"
fi

CHECK_JAVA_VERSION_OUTPUT=$("$JAVA" -version 2>&1)
JAVA_VERSION=$(echo $CHECK_JAVA_VERSION_OUTPUT | awk -F\" '/version/{print $2}')
if [ -z "$JAVA_VERSION" ]; then
  echo "Check Java Version failed. $CHECK_JAVA_VERSION_OUTPUT"
  exit 1
fi

JAVA_MAJOR_VERSION=$(echo $JAVA_VERSION | awk -F "." '{print $1}')
if [ -z "$JAVA_MAJOR_VERSION" ]; then
  echo "Parse java major version failed with java version $JAVA_VERSION"
  exit 1
fi

if [[ $(expr $JAVA_MAJOR_VERSION) -lt 17 ]]; then
  echo "Too old Java version $JAVA_VERSION, at least Java 17 is required"
  exit 1
fi

echo "Using Java Version ${JAVA_VERSION} locating at '${JAVA}'"
MEMORY=$(total_memory)
echo -e "Total Memory: $(awk -v mem="$MEMORY" 'BEGIN {printf "%.1f", mem/1024/1024/1024}') GB\n"

# Perf options
if [ -z "$JVM_PERF_OPTS" ]; then
  JVM_PERF_OPTS="-server -XX:MaxInlineLevel=15 -Djava.awt.headless=true"
fi

# GC options
if [ -z "$JVM_GC_OPTS" ]; then
  JVM_GC_OPTS="-XX:+UnlockExperimentalVMOptions \
  -XX:+UnlockDiagnosticVMOptions \
  -XX:+UseZGC \
  -XX:ZAllocationSpikeTolerance=5 \
  -Xlog:async \
  -Xlog:gc:file='${LOG_DIR}/gc-%t.log:time,tid,tags:filecount=5,filesize=50m' \
  -XX:+CrashOnOutOfMemoryError \
  -XX:+HeapDumpOnOutOfMemoryError \
  -XX:HeapDumpPath='${LOG_DIR}' \
"
fi

eval JVM_GC=("$JVM_GC_OPTS")
# Memory options
if [ -z "$JVM_HEAP_OPTS" ]; then
  MEMORY_FRACTION=70 # Percentage of total memory to use
  HEAP_MEMORY=$(($MEMORY / 100 * $MEMORY_FRACTION))
  SOFT_MAX_HEAP_MEMORY=$(($HEAP_MEMORY * 80 / 100))
  MIN_HEAP_MEMORY=$(($HEAP_MEMORY / 2))

  # Calculate max direct memory based on total memory
  MAX_DIRECT_MEMORY_FRACTION=20 # Percentage of total memory to use for max direct memory
  MAX_DIRECT_MEMORY=$(($MEMORY / 100 * $MAX_DIRECT_MEMORY_FRACTION))

  META_SPACE_MEMORY=128m
  MAX_META_SPACE_MEMORY=500m

  # Set heap options
  JVM_HEAP_OPTS="-Xms$(memory_in_mb ${MIN_HEAP_MEMORY})m -Xmx$(memory_in_mb ${HEAP_MEMORY})m -XX:SoftMaxHeapSize=$(memory_in_mb ${SOFT_MAX_HEAP_MEMORY})m -XX:MetaspaceSize=${META_SPACE_MEMORY} -XX:MaxMetaspaceSize=${MAX_META_SPACE_MEMORY} -XX:MaxDirectMemorySize=${MAX_DIRECT_MEMORY}"
fi

EXTRA_JVM_OPTS="$EXTRA_JVM_OPTS -Dio.netty.tryReflectionSetAccessible=true -Dio.netty.allocator.cacheTrimIntervalMillis=5000 --add-opens java.base/java.nio=ALL-UNNAMED"

# Set Debug options if enabled
if [ "x$JVM_DEBUG" = "xtrue" ]; then
  DEFAULT_JAVA_DEBUG_PORT="8008"
  if [ -z "$JAVA_DEBUG_PORT" ]; then
    JAVA_DEBUG_PORT="$DEFAULT_JAVA_DEBUG_PORT"
  fi
  DEFAULT_JAVA_DEBUG_OPTS="-Djava.net.preferIPv4Stack=true -agentlib:jdwp=transport=dt_socket,server=y,suspend=${DEBUG_SUSPEND_FLAG:-n},address=*:$JAVA_DEBUG_PORT"
  if [ -z "$JAVA_DEBUG_OPTS" ]; then
    JAVA_DEBUG_OPTS="$DEFAULT_JAVA_DEBUG_OPTS"
  fi
  echo "Enabling Java debug options: $JAVA_DEBUG_OPTS"
  EXTRA_JVM_OPTS="$JAVA_DEBUG_OPTS $EXTRA_JVM_OPTS"
fi

# Enable core dump generation
ulimit -c unlimited

if [ "x$FOREGROUND_MODE" = "xtrue" ]; then
  exec "$JAVA" $JVM_HEAP_OPTS $JVM_PERF_OPTS "${JVM_GC[@]}" $EXTRA_JVM_OPTS \
    -cp "$CLASSPATH" \
    -DLOG_DIR="$LOG_DIR" \
    -DCONF_DIR="$CONF_DIR" \
    -DDATA_DIR="$DATA_DIR" \
    -Dlog4j.configurationFile="$LOG_CONF_FILE" \
    -Dpf4j.pluginsDir="$PLUGIN_DIR" \
    $NAME -c "$CONF_FILE"
else
  nohup "$JAVA" $JVM_HEAP_OPTS $JVM_PERF_OPTS "${JVM_GC[@]}" $EXTRA_JVM_OPTS \
    -cp "$CLASSPATH" \
    -DLOG_DIR="$LOG_DIR" \
    -DCONF_DIR="$CONF_DIR" \
    -DDATA_DIR="$DATA_DIR" \
    -Dlog4j.configurationFile="$LOG_CONF_FILE" \
    -Dpf4j.pluginsDir="$PLUGIN_DIR" \
    $NAME -c "$CONF_FILE" >"${LOG_DIR}/stdout.log" 2>&1 </dev/null &
  PIDS=$!
  echo "$PIDS" > "$PID_FILE"
  echo "$NAME process started: $PIDS"
fi