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
if "%2" == "" (
    echo "Usage: %0 ReturnVar ProcessName"
    exit /b 1
)
set PID_FILE=pid
wmic process where "commandline like '%%%2%%' and not name='wmic.exe'" get processId 2>^&1 | find /v /i "processId" >%PID_FILE%
set PID=
for /f "tokens=*" %%a in (!PID_FILE!) do (
    if [!PID!] EQU [] (
        set PID=%%a
    )
)
del %PID_FILE%
for /f "delims=0123456789 " %%a in ("%PID%") do (
    endlocal & set %1=
    exit /b 1
)

endlocal & set %1=%PID%
