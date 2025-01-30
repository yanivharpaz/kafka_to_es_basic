@rem
@rem Copyright 2015 the original author or authors.
@rem
@rem Licensed under the Apache License, Version 2.0 (the "License");
@rem you may not use this file except in compliance with the License.
@rem You may obtain a copy of the License at
@rem
@rem      https://www.apache.org/licenses/LICENSE-2.0
@rem
@rem Unless required by applicable law or agreed to in writing, software
@rem distributed under the License is distributed on an "AS IS" BASIS,
@rem WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
@rem See the License for the specific language governing permissions and
@rem limitations under the License.
@rem

@if "%DEBUG%"=="" @echo off
@rem ##########################################################################
@rem
@rem  kafka_to_es_basic startup script for Windows
@rem
@rem ##########################################################################

@rem Set local scope for the variables with windows NT shell
if "%OS%"=="Windows_NT" setlocal

set DIRNAME=%~dp0
if "%DIRNAME%"=="" set DIRNAME=.
@rem This is normally unused
set APP_BASE_NAME=%~n0
set APP_HOME=%DIRNAME%..

@rem Resolve any "." and ".." in APP_HOME to make it shorter.
for %%i in ("%APP_HOME%") do set APP_HOME=%%~fi

@rem Add default JVM options here. You can also use JAVA_OPTS and KAFKA_TO_ES_BASIC_OPTS to pass JVM options to this script.
set DEFAULT_JVM_OPTS=

@rem Find java.exe
if defined JAVA_HOME goto findJavaFromJavaHome

set JAVA_EXE=java.exe
%JAVA_EXE% -version >NUL 2>&1
if %ERRORLEVEL% equ 0 goto execute

echo. 1>&2
echo ERROR: JAVA_HOME is not set and no 'java' command could be found in your PATH. 1>&2
echo. 1>&2
echo Please set the JAVA_HOME variable in your environment to match the 1>&2
echo location of your Java installation. 1>&2

goto fail

:findJavaFromJavaHome
set JAVA_HOME=%JAVA_HOME:"=%
set JAVA_EXE=%JAVA_HOME%/bin/java.exe

if exist "%JAVA_EXE%" goto execute

echo. 1>&2
echo ERROR: JAVA_HOME is set to an invalid directory: %JAVA_HOME% 1>&2
echo. 1>&2
echo Please set the JAVA_HOME variable in your environment to match the 1>&2
echo location of your Java installation. 1>&2

goto fail

:execute
@rem Setup the command line

set CLASSPATH=%APP_HOME%\lib\kafka_to_es_basic-1.0-SNAPSHOT.jar;%APP_HOME%\lib\elasticsearch-rest-high-level-client-6.8.0.jar;%APP_HOME%\lib\elasticsearch-6.8.0.jar;%APP_HOME%\lib\elasticsearch-rest-client-6.8.0.jar;%APP_HOME%\lib\transport-netty4-client-6.8.0.jar;%APP_HOME%\lib\connect-api-3.5.1.jar;%APP_HOME%\lib\kafka-clients-3.5.1.jar;%APP_HOME%\lib\jackson-annotations-2.15.2.jar;%APP_HOME%\lib\elasticsearch-x-content-6.8.0.jar;%APP_HOME%\lib\jackson-core-2.15.2.jar;%APP_HOME%\lib\jackson-dataformat-cbor-2.15.2.jar;%APP_HOME%\lib\jackson-dataformat-smile-2.15.2.jar;%APP_HOME%\lib\jackson-dataformat-yaml-2.15.2.jar;%APP_HOME%\lib\jackson-databind-2.15.2.jar;%APP_HOME%\lib\logback-classic-1.2.11.jar;%APP_HOME%\lib\logback-core-1.2.11.jar;%APP_HOME%\lib\log4j-to-slf4j-2.17.1.jar;%APP_HOME%\lib\slf4j-api-1.7.36.jar;%APP_HOME%\lib\log4j-core-2.17.1.jar;%APP_HOME%\lib\log4j-api-2.17.1.jar;%APP_HOME%\lib\parent-join-client-6.8.0.jar;%APP_HOME%\lib\aggs-matrix-stats-client-6.8.0.jar;%APP_HOME%\lib\rank-eval-client-6.8.0.jar;%APP_HOME%\lib\lang-mustache-client-6.8.0.jar;%APP_HOME%\lib\elasticsearch-cli-6.8.0.jar;%APP_HOME%\lib\elasticsearch-core-6.8.0.jar;%APP_HOME%\lib\elasticsearch-secure-sm-6.8.0.jar;%APP_HOME%\lib\lucene-core-7.7.0.jar;%APP_HOME%\lib\lucene-analyzers-common-7.7.0.jar;%APP_HOME%\lib\lucene-backward-codecs-7.7.0.jar;%APP_HOME%\lib\lucene-grouping-7.7.0.jar;%APP_HOME%\lib\lucene-highlighter-7.7.0.jar;%APP_HOME%\lib\lucene-join-7.7.0.jar;%APP_HOME%\lib\lucene-memory-7.7.0.jar;%APP_HOME%\lib\lucene-misc-7.7.0.jar;%APP_HOME%\lib\lucene-queries-7.7.0.jar;%APP_HOME%\lib\lucene-queryparser-7.7.0.jar;%APP_HOME%\lib\lucene-sandbox-7.7.0.jar;%APP_HOME%\lib\lucene-spatial-7.7.0.jar;%APP_HOME%\lib\lucene-spatial-extras-7.7.0.jar;%APP_HOME%\lib\lucene-spatial3d-7.7.0.jar;%APP_HOME%\lib\lucene-suggest-7.7.0.jar;%APP_HOME%\lib\hppc-0.7.1.jar;%APP_HOME%\lib\joda-time-2.10.1.jar;%APP_HOME%\lib\t-digest-3.2.jar;%APP_HOME%\lib\HdrHistogram-2.1.9.jar;%APP_HOME%\lib\jna-4.5.1.jar;%APP_HOME%\lib\httpclient-4.5.2.jar;%APP_HOME%\lib\httpcore-4.4.5.jar;%APP_HOME%\lib\httpasyncclient-4.1.2.jar;%APP_HOME%\lib\httpcore-nio-4.4.5.jar;%APP_HOME%\lib\commons-codec-1.10.jar;%APP_HOME%\lib\commons-logging-1.1.3.jar;%APP_HOME%\lib\netty-buffer-4.1.32.Final.jar;%APP_HOME%\lib\netty-codec-4.1.32.Final.jar;%APP_HOME%\lib\netty-codec-http-4.1.32.Final.jar;%APP_HOME%\lib\netty-common-4.1.32.Final.jar;%APP_HOME%\lib\netty-handler-4.1.32.Final.jar;%APP_HOME%\lib\netty-resolver-4.1.32.Final.jar;%APP_HOME%\lib\netty-transport-4.1.32.Final.jar;%APP_HOME%\lib\zstd-jni-1.5.5-1.jar;%APP_HOME%\lib\lz4-java-1.8.0.jar;%APP_HOME%\lib\snappy-java-1.1.10.1.jar;%APP_HOME%\lib\javax.ws.rs-api-2.1.1.jar;%APP_HOME%\lib\compiler-0.9.3.jar;%APP_HOME%\lib\snakeyaml-1.17.jar;%APP_HOME%\lib\jopt-simple-5.0.2.jar


@rem Execute kafka_to_es_basic
"%JAVA_EXE%" %DEFAULT_JVM_OPTS% %JAVA_OPTS% %KAFKA_TO_ES_BASIC_OPTS%  -classpath "%CLASSPATH%" org.example.KafkaToElasticsearchConsumer %*

:end
@rem End local scope for the variables with windows NT shell
if %ERRORLEVEL% equ 0 goto mainEnd

:fail
rem Set variable KAFKA_TO_ES_BASIC_EXIT_CONSOLE if you need the _script_ return code instead of
rem the _cmd.exe /c_ return code!
set EXIT_CODE=%ERRORLEVEL%
if %EXIT_CODE% equ 0 set EXIT_CODE=1
if not ""=="%KAFKA_TO_ES_BASIC_EXIT_CONSOLE%" exit %EXIT_CODE%
exit /b %EXIT_CODE%

:mainEnd
if "%OS%"=="Windows_NT" endlocal

:omega
