@REM
@REM Copyright (c) 2012 - 2016 YCSB contributors. All rights reserved.
@REM
@REM Licensed under the Apache License, Version 2.0 (the "License"); you
@REM may not use this file except in compliance with the License. You
@REM may obtain a copy of the License at
@REM
@REM http://www.apache.org/licenses/LICENSE-2.0
@REM
@REM Unless required by applicable law or agreed to in writing, software
@REM distributed under the License is distributed on an "AS IS" BASIS,
@REM WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
@REM implied. See the License for the specific language governing
@REM permissions and limitations under the License. See accompanying
@REM LICENSE file.
@REM
@REM -----------------------------------------------------------------------
@REM Control Script for YCSB
@REM
@REM Environment Variable Prerequisites
@REM
@REM   Do not set the variables in this script. Instead put them into a script
@REM   setenv.sh in YCSB_HOME/bin to keep your customizations separate.
@REM
@REM   YCSB_HOME       (Optional) YCSB installation directory.  If not set
@REM                   this script will use the parent directory of where this
@REM                   script is run from.
@REM
@REM   JAVA_HOME       (Required) Must point at your Java Development Kit
@REM                   or Java Runtime Environment installation.
@REM
@REM   JAVA_OPTS       (Optional) Java runtime options used when any command
@REM                   is executed.
@REM
@REM   WARNING!!! YCSB home must be located in a directory path that doesn't
@REM   contain spaces.
@REM

@ECHO OFF
SETLOCAL ENABLEDELAYEDEXPANSION

@REM Only set YCSB_HOME if not already set
PUSHD %~dp0..
IF NOT DEFINED YCSB_HOME SET YCSB_HOME=%CD%
POPD

@REM Ensure that any extra CLASSPATH variables are set via setenv.bat
SET CLASSPATH=

@REM Pull in customization options
if exist "%YCSB_HOME%\bin\setenv.bat" call "%YCSB_HOME%\bin\setenv.bat"

@REM Check if we have a usable JDK
IF "%JAVA_HOME%." == "." GOTO noJavaHome
IF NOT EXIST "%JAVA_HOME%\bin\java.exe" GOTO noJavaHome
GOTO okJava
:noJavaHome
ECHO The JAVA_HOME environment variable is not defined correctly.
GOTO exit
:okJava

@REM Determine YCSB command argument
IF NOT "load" == "%1" GOTO noload
SET YCSB_COMMAND=-load
SET YCSB_CLASS=com.yahoo.ycsb.Client
GOTO gotCommand
:noload
IF NOT "run" == "%1" GOTO noRun
SET YCSB_COMMAND=-t
SET YCSB_CLASS=com.yahoo.ycsb.Client
GOTO gotCommand
:noRun
IF NOT "shell" == "%1" GOTO noShell
SET YCSB_COMMAND=
SET YCSB_CLASS=com.yahoo.ycsb.CommandLine
GOTO gotCommand
:noShell
ECHO [ERROR] Found unknown command '%1'
ECHO [ERROR] Expected one of 'load', 'run', or 'shell'. Exiting.
GOTO exit
:gotCommand

@REM Find binding information
FOR /F "delims=" %%G in (
  'FINDSTR /B "%2:" %YCSB_HOME%\bin\bindings.properties'
) DO SET "BINDING_LINE=%%G"

IF NOT "%BINDING_LINE%." == "." GOTO gotBindingLine
ECHO [ERROR] The specified binding '%2' was not found.  Exiting.
GOTO exit
:gotBindingLine

@REM Pull out binding name and class
FOR /F "tokens=1-2 delims=:" %%G IN ("%BINDING_LINE%") DO (
  SET BINDING_NAME=%%G
  SET BINDING_CLASS=%%H
)

@REM Some bindings have multiple versions that are managed in the same
@REM directory.
@REM   They are noted with a '-' after the binding name.
@REM   (e.g. cassandra-7 & cassandra-8)
FOR /F "tokens=1 delims=-" %%G IN ("%BINDING_NAME%") DO (
  SET BINDING_DIR=%%G
)

@REM The 'basic' binding is core functionality
IF NOT "%BINDING_NAME%" == "basic" GOTO noBasic
SET BINDING_DIR=core
:noBasic

@REM Add Top level conf to classpath
IF "%CLASSPATH%." == "." GOTO emptyClasspath
SET CLASSPATH=%CLASSPATH%;%YCSB_HOME%\conf
GOTO confAdded
:emptyClasspath
SET CLASSPATH=%YCSB_HOME%\conf
:confAdded

@REM Accumulo deprecation message
IF NOT "%BINDING_DIR%" == "accumulo" GOTO notAliasAccumulo
echo [WARN] The 'accumulo' client has been deprecated in favor of version specific bindings. This name still maps to the binding for Accumulo 1.6, which is named 'accumulo-1.6'. This alias will be removed in a future YCSB release.
SET BINDING_DIR=accumulo1.6
:notAliasAccumulo

@REM Accumulo 1.6 deprecation message
IF NOT "%BINDING_DIR%" == "accumulo1.6" GOTO notAccumulo16
echo [WARN] The 'accumulo1.6' client has been deprecated because Accumulo 1.6 is EOM. If you are using Accumulo 1.7+ try using the 'accumulo1.7' client instead.
:notAccumulo16

@REM Cassandra2 deprecation message
IF NOT "%BINDING_DIR%" == "cassandra2" GOTO notAliasCassandra
echo [WARN] The 'cassandra2-cql' client has been deprecated. It has been renamed to simply 'cassandra-cql'. This alias will be removed in the next YCSB release.
SET BINDING_DIR=cassandra
:notAliasCassandra

@REM Build classpath according to source checkout or release distribution
IF EXIST "%YCSB_HOME%\pom.xml" GOTO gotSource

@REM Core libraries
FOR %%F IN (%YCSB_HOME%\lib\*.jar) DO (
  SET CLASSPATH=!CLASSPATH!;%%F%
)

@REM Database conf dir
IF NOT EXIST "%YCSB_HOME%\%BINDING_DIR%-binding\conf" GOTO noBindingConf
set CLASSPATH=%CLASSPATH%;%YCSB_HOME%\%BINDING_DIR%-binding\conf
:noBindingConf

@REM Database libraries
FOR %%F IN (%YCSB_HOME%\%BINDING_DIR%-binding\lib\*.jar) DO (
  SET CLASSPATH=!CLASSPATH!;%%F%
)
GOTO classpathComplete

:gotSource
@REM Check for some basic libraries to see if the source has been built.
IF EXIST "%YCSB_HOME%\%BINDING_DIR%\target\*.jar" GOTO gotJars

@REM Call mvn to build source checkout.
IF "%BINDING_NAME%" == "basic" GOTO buildCore
SET MVN_PROJECT=%BINDING_DIR%-binding
goto gotMvnProject
:buildCore
SET MVN_PROJECT=core
:gotMvnProject

ECHO [WARN] YCSB libraries not found.  Attempting to build...
CALL mvn -pl com.yahoo.ycsb:%MVN_PROJECT% -am package -DskipTests
IF %ERRORLEVEL% NEQ 0 (
  ECHO [ERROR] Error trying to build project. Exiting.
  GOTO exit
)

:gotJars
@REM Core libraries
FOR %%F IN (%YCSB_HOME%\core\target\*.jar) DO (
  SET CLASSPATH=!CLASSPATH!;%%F%
)

@REM Database conf (need to find because location is not consistent)
FOR /D /R %YCSB_HOME%\%BINDING_DIR% %%F IN (*) DO (
  IF "%%~nxF" == "conf" SET CLASSPATH=!CLASSPATH!;%%F%
)

@REM Database libraries
FOR %%F IN (%YCSB_HOME%\%BINDING_DIR%\target\*.jar) DO (
  SET CLASSPATH=!CLASSPATH!;%%F%
)

@REM Database dependency libraries
FOR %%F IN (%YCSB_HOME%\%BINDING_DIR%\target\dependency\*.jar) DO (
  SET CLASSPATH=!CLASSPATH!;%%F%
)

:classpathComplete

@REM Couchbase deprecation message
IF NOT "%BINDING_DIR%" == "couchbase" GOTO notOldCouchbase
echo [WARN] The 'couchbase' client is deprecated. If you are using Couchbase 4.0+ try using the 'couchbase2' client instead.
:notOldCouchbase

@REM HBase 0.98 deprecation message
IF NOT "%BINDING_DIR%" == "hbase098" GOTO not098HBase
echo [WARN] The 'hbase098' client is deprecated because HBase 0.98 is EOM. If you are using HBase 1.2+ try using the 'hbase12' client instead.
:not098HBase

@REM HBase 1.0 deprecation message
IF NOT "%BINDING_DIR%" == "hbase10" GOTO not10HBase
echo [WARN] The 'hbase10' client is deprecated because HBase 1.0 is EOM. If you are using HBase 1.2+ try using the 'hbase12' client instead.
:not10HBase

@REM Get the rest of the arguments, skipping the first 2
FOR /F "tokens=2*" %%G IN ("%*") DO (
  SET YCSB_ARGS=%%H
)

@REM Run YCSB
@ECHO ON
"%JAVA_HOME%\bin\java.exe" %JAVA_OPTS% -classpath "%CLASSPATH%" %YCSB_CLASS% %YCSB_COMMAND% -db %BINDING_CLASS% %YCSB_ARGS%
@ECHO OFF

GOTO end

:exit
EXIT /B 1;

:end

