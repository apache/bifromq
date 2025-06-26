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
if [%1] EQU [] (
    echo Usage: %0 Name
    exit /b 1
)
set PID=
call "%~dp0pid.bat" PID %1
if "%PID%" == "" (
    echo No %1 to stop
    exit /b 1
)
echo Find %1 process %PID%, and stopping it
wmic process where "processid=%PID%" delete
echo %1 process %PID% was stopped
endlocal
