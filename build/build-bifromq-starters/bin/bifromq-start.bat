::
:: Licensed to the Apache Software Foundation (ASF) under one
:: or more contributor license agreements.  See the NOTICE file
:: distributed with this work for additional information
:: regarding copyright ownership.  The ASF licenses this file
:: to you under the Apache License, Version 2.0 (the
:: "License"); you may not use this file except in compliance
:: with the License.  You may obtain a copy of the License at
::
::   http://www.apache.org/licenses/LICENSE-2.0
::
:: Unless required by applicable law or agreed to in writing,
:: software distributed under the License is distributed on an
:: "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
:: KIND, either express or implied.  See the License for the
:: specific language governing permissions and limitations
:: under the License.


@echo off
setlocal enabledelayedexpansion

if "%4" == "" (
   echo "USAGE: %0 -c classname -f filename [-fg]"
   exit /b 1
)

:parseCommand
if "%1" == "-fg" (
  set FOREGROUND_MODE=true
  shift
  goto parseCommand
) else if "%1" == "-c" (
  set NAME=%2
  shift
  shift
  goto parseCommand
) else if "%1" == "-f" (
  set FILE_NAME=%2
  shift
  shift
  goto parseCommand
)

set BIN_DIR=%~dp0
for %%i in ("%BIN_DIR%\..") do @(
  set BASE_DIR=%%~fi
)
set CONF_DIR=%BASE_DIR%\conf
set CONF_FILE=%CONF_DIR%\%FILE_NAME%
set PLUGIN_DIR=%BASE_DIR%\plugins
set LOG_CONFIG_FILE=%CONF_DIR%\log4j2.xml
set LIB_DIR=%BASE_DIR%\lib
set CLASSPATH=%LIB_DIR%\*

rem Log directory to use
if "" == "%LOG_DIR%" set LOG_DIR=%BASE_DIR%\logs
rem create logs directory
if not exist "%LOG_DIR%" (
    mkdir "%LOG_DIR%"
)

rem data directory to use
if "" == "%DATA_DIR%" set DATA_DIR=%BASE_DIR%\data
rem create data directory
if not exist "%DATA_DIR%" (
    mkdir "%DATA_DIR%"
)

call "%~dp0pid.bat" PID %NAME%
if defined PID (
    echo %NAME% already started: %PID%
    exit /b 1
)

if "" == "%BIND_ADDR%" (
   for /f "delims=: tokens=2" %%i in ('ipconfig ^| find /i "ipv4"') do (
         set BIND_ADDR=%%i
         set BIND_ADDR=!BIND_ADDR: =!
   )
)

rem check java version
if "" == "%JAVA_HOME%" (
    set JAVA_COMMAND="java"
    if "true" == "%FOREGROUND_MODE%" (
        set JAVA="java"
    ) else (
        set JAVA="javaw"
    )
) else (
    set JAVA_COMMAND="%JAVA_HOME%\bin\java"
    if "true" == "%FOREGROUND_MODE%" (
        set JAVA="%JAVA_HOME%\bin\java"
    ) else (
        set JAVA="%JAVA_HOME%\bin\javaw"
    )
)

for /f "usebackq tokens=*" %%a in (`"%JAVA_COMMAND%" -version 2^>^&1 `) do (
     set CHECK_JAVA_VERSION_OUTPUT=!CHECK_JAVA_VERSION_OUTPUT!%%a
     for /f "usebackq tokens=3 delims= " %%b in (`echo %%a ^|findstr /i version `) do (
        set JAVA_VERSION=%%b
            rem remove ""
            set JAVA_VERSION=!JAVA_VERSION:~1,-1!
            for /f "usebackq tokens=1 delims=." %%c in (`echo !JAVA_VERSION!`) do (
                set /a JAVA_MAJOR_VERSION=%%c
            )
     )
)
if "" == "%JAVA_MAJOR_VERSION%" (
    echo "Using %JAVA_COMMAND% check java version failed. %CHECK_JAVA_VERSION_OUTPUT% "
    exit /b 1
)
if %JAVA_MAJOR_VERSION% LSS 17 (
    echo "Too old Java version %JAVA_MAJOR_VERSION%, at least Java 17 is required"
    exit /b 1
)
echo "Using Java Version %JAVA_VERSION% locating at %JAVA_COMMAND%"

call :total_memory_in_kb MEMORY %MEM_LIMIT%
echo "Total Memory: %MEMORY% KB"

rem Perf options
if "" == "%JVM_PERF_OPTS%" set JVM_PERF_OPTS="-server -XX:MaxInlineLevel=15 -Djava.awt.headless=true"

rem GC options
if "" == "%JVM_GC_OPTS%" (
   set JVM_GC_OPTS='-XX:+UnlockExperimentalVMOptions' ^
                        '-XX:+UnlockDiagnosticVMOptions' ^
                        '-XX:+UseZGC' ^
                        '-XX:ZAllocationSpikeTolerance=5' ^
                        '-XX:+HeapDumpOnOutOfMemoryError' ^
                        '-XX:HeapDumpPath="%LOG_DIR%"' ^
                        '-Xlog:async' ^
                        '-Xlog:gc:file="%LOG_DIR%\gc.log:time,tid,tags:filecount=5,filesize=50m"'
)

rem Memory options
if "" == "%JVM_HEAP_OPTS%" (
    set MEMORY_FRACTION=70
    set /a HEAP_MEMORY=!MEMORY!/100*!MEMORY_FRACTION!
    set /a MIN_HEAP_MEMORY=!HEAP_MEMORY!/2
    set /a SOFT_MAX_HEAP_MEMORY=!HEAP_MEMORY! * 80 / 100

    rem Calculate max direct memory based on total memory
    rem Percentage of total memory to use for max direct memory
    set MAX_DIRECT_MEMORY_FRACTION=20
    set /a MAX_DIRECT_MEMORY=!MEMORY!/100*!MAX_DIRECT_MEMORY_FRACTION!

    set META_SPACE_MEMORY=128m
    set MAX_META_SPACE_MEMORY=500m
    call :memory_in_mb XMS !MIN_HEAP_MEMORY!
    call :memory_in_mb XMX !HEAP_MEMORY!
    call :memory_in_mb SXMX !SOFT_MAX_HEAP_MEMORY!
    set JVM_HEAP_OPTS="-Xms!XMS!m -Xmx!XMX!m -XX:SoftMaxHeapSize=!SXMX!m -XX:MetaspaceSize=!META_SPACE_MEMORY! -XX:MaxMetaspaceSize=!MAX_META_SPACE_MEMORY! -XX:MaxDirectMemorySize=!MAX_DIRECT_MEMORY!"
)

rem Generic jvm settings you want to add
if "" == "%EXTRA_JVM_OPTS%" (
    set EXTRA_JVM_OPTS=""
)

rem Set Debug options if enabled
if "" == "%JVM_DEBUG%" (
    rem do nothing
) else (
    set DEFAULT_JAVA_DEBUG_PORT="8008"
    if "" == "!JAVA_DEBUG_PORT!" (
        set JAVA_DEBUG_PORT=!DEFAULT_JAVA_DEBUG_PORT!
    )
    rem Use the defaults if JAVA_DEBUG_OPTS was not set
    if not defined DEBUG_SUSPEND_FLAG set DEBUG_SUSPEND_FLAG=n
    set DEFAULT_JAVA_DEBUG_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=!DEBUG_SUSPEND_FLAG!,address=*:!JAVA_DEBUG_PORT!"
    if "" == "!JAVA_DEBUG_OPTS!" (
        set JAVA_DEBUG_OPTS=!DEFAULT_JAVA_DEBUG_OPTS:"=!
    )
    echo "Enabling Java debug options: !JAVA_DEBUG_OPTS!"
    set EXTRA_JVM_OPTS=!JAVA_DEBUG_OPTS! !EXTRA_JVM_OPTS:"=!
)

%JAVA% %JVM_HEAP_OPTS:"=% ^
      %JVM_PERF_OPTS:"=% ^
      %JVM_GC_OPTS:'=% ^
      %EXTRA_JVM_OPTS:"=% ^
      -cp "%CLASSPATH%" ^
      -DLOG_DIR="%LOG_DIR%" ^
      -DCONF_DIR="%CONF_DIR%" ^
      -DDATA_DIR="%DATA_DIR%" ^
      -DBIND_ADDR=%BIND_ADDR% ^
      -Dlog4j.configurationFile="%LOG_CONFIG_FILE%" ^
      -Dpf4j.pluginsDir="%PLUGIN_DIR%" ^
       %NAME% ^
      -c "%CONF_FILE%"
exit /b 0
endlocal


goto :eof

:total_memory_in_kb
   if [%2] EQU [] (
      for /f "skip=1" %%i in ('wmic os get TotalVisibleMemorySize') do (
        if %%i geq 0 (
           set /a %1=%%i
        )
      )
   ) else (
      set %1=%2
   )
   goto :eof
:memory_in_mb
    set /a %1=%2/1024
    goto :eof
:memory_in_gb
    set /a %1=%2/1024/1024
    goto :eof
:pid
    wmic process where "commandline like '%%%2%%'" get processid | find /v /i "processid" 2^>^&1
    goto :eof
