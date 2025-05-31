::
:: Copyright (c) 2023. The BifroMQ Authors. All Rights Reserved.
::
:: Licensed under the Apache License, Version 2.0 (the "License");
:: you may not use this file except in compliance with the License.
:: You may obtain a copy of the License at
::    http://www.apache.org/licenses/LICENSE-2.0
:: Unless required by applicable law or agreed to in writing,
:: software distributed under the License is distributed on an "AS IS" BASIS,
:: WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
:: See the License for the specific language governing permissions and limitations under the License.
::

@echo off

setlocal enabledelayedexpansion
if [%1] EQU [] (b
    call :usage %0
    exit /b 1
)
set BIN_DIR=%~dp0
for %%i in ("%BIN_DIR%\..") do (
  set BASE_DIR=%%~fi
)
set LOG_DIR=%BASE_DIR%\logs
set SCRIPT=%0
set COMMAND=%1
set FOREGROUND=%2
set NAME=org.apache.bifromq.starter.StandaloneStarter
if "start" == "%COMMAND%" (
    call "%~dp0bifromq-start.bat" -c %NAME% -f standalone.yml %FOREGROUND%
) else if "stop" == "%COMMAND%" (
    call "%~dp0bifromq-stop.bat" %NAME%
) else if "restart" == "%COMMAND%" (
    call "%~dp0bifromq-stop.bat" %NAME%
    call "%~dp0bifromq-start.bat" -c %NAME% -f standalone.yml %FOREGROUND%
) else (
    call :usage %SCRIPT%
    exit /b 1
)
endlocal
goto :eof

:usage
    echo USAGE: %1 {start|stop|restart} [-fg]
    goto :eof
