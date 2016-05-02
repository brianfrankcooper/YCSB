#!/bin/sh
#
# Copyright (c) 2012 - 2016 YCSB contributors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you
# may not use this file except in compliance with the License. You
# may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied. See the License for the specific language governing
# permissions and limitations under the License. See accompanying
# LICENSE file.
#
# -----------------------------------------------------------------------------
# Control Script for YCSB
#
# Environment Variable Prerequisites
#
#   Do not set the variables in this script. Instead put them into a script
#   setenv.sh in YCSB_HOME/bin to keep your customizations separate.
#
#   YCSB_HOME       (Optional) YCSB installation directory.  If not set
#                   this script will use the parent directory of where this
#                   script is run from.
#
#   JAVA_HOME       (Optional) Must point at your Java Development Kit
#                   installation.  If empty, this script tries use the
#                   available java executable.
#
#   JAVA_OPTS       (Optional) Java runtime options used when any command
#                   is executed.
#
#   WARNING!!! YCSB home must be located in a directory path that doesn't
#   contain spaces.
#

# Cygwin support
CYGWIN=false
case "`uname`" in
CYGWIN*) CYGWIN=true;;
esac

# Get script path
SCRIPT_DIR=`dirname "$0" 2>/dev/null`

# Only set YCSB_HOME if not already set
[ -z "$YCSB_HOME" ] && YCSB_HOME=`cd "$SCRIPT_DIR/.." 2>/dev/null; pwd`

# Ensure that any extra CLASSPATH variables are set via setenv.sh
CLASSPATH=

# Pull in customization options
if [ -r "$YCSB_HOME/bin/setenv.sh" ]; then
  . "$YCSB_HOME/bin/setenv.sh"
fi

# Attempt to find the available JAVA, if JAVA_HOME not set
if [ -z "$JAVA_HOME" ]; then
  JAVA_PATH=`which java 2>/dev/null`
  if [ "x$JAVA_PATH" != "x" ]; then
    JAVA_HOME=`dirname $(dirname "$JAVA_PATH" 2>/dev/null)`
  fi
fi

# Determine YCSB command argument
if [ "load" = "$1" ] ; then
  YCSB_COMMAND=-load
  YCSB_CLASS=com.yahoo.ycsb.Client
elif [ "run" = "$1" ] ; then
  YCSB_COMMAND=-t
  YCSB_CLASS=com.yahoo.ycsb.Client
elif [ "shell" = "$1" ] ; then
  YCSB_COMMAND=
  YCSB_CLASS=com.yahoo.ycsb.CommandLine
else
  echo "[ERROR] Found unknown command '$1'"
  echo "[ERROR] Expected one of 'load', 'run', or 'shell'. Exiting."
  exit 1;
fi

# Find binding information
BINDING_LINE=`grep "^$2:" "$YCSB_HOME/bin/bindings.properties" -m 1`

if [ -z $BINDING_LINE ] ; then
  echo "[ERROR] The specified binding '$2' was not found.  Exiting."
  exit 1;
fi

# Get binding name and class
BINDING_NAME=`echo $BINDING_LINE | cut -d':' -f1`
BINDING_CLASS=`echo $BINDING_LINE | cut -d':' -f2`

# Some bindings have multiple versions that are managed in the same directory.
#   They are noted with a '-' after the binding name.
#   (e.g. cassandra-7 & cassandra-8)
BINDING_DIR=`echo $BINDING_NAME | cut -d'-' -f1`

# The 'basic' binding is core functionality
if [ $BINDING_NAME = "basic" ] ; then
  BINDING_DIR=core
fi

# For Cygwin, ensure paths are in UNIX format before anything is touched
if $CYGWIN; then
  [ -n "$JAVA_HOME" ] && JAVA_HOME=`cygpath --unix "$JAVA_HOME"`
  [ -n "$CLASSPATH" ] && CLASSPATH=`cygpath --path --unix "$CLASSPATH"`
fi

# Check if source checkout, or release distribution
DISTRIBUTION=true
if [ -r "$YCSB_HOME/pom.xml" ]; then
  DISTRIBUTION=false;
fi

# Add Top level conf to classpath
if [ -z "$CLASSPATH" ] ; then
  CLASSPATH="$YCSB_HOME/conf"
else
  CLASSPATH="$CLASSPATH:$YCSB_HOME/conf"
fi

# Build classpath
if $DISTRIBUTION; then
  # Core libraries
  for f in `ls "$YCSB_HOME"/lib/*.jar`; do
    CLASSPATH="$CLASSPATH:$f"
  done

  # Database conf dir
  if [ -r "$YCSB_HOME/$BINDING_DIR-binding/conf" ] ; then
    CLASSPATH="$CLASSPATH:$YCSB_HOME/$BINDING_DIR-binding/conf"
  fi

  # Database libraries
  for f in `ls "$YCSB_HOME"/$BINDING_DIR-binding/lib/*.jar 2>/dev/null`; do
    CLASSPATH="$CLASSPATH:$f"
  done

else
  # Core libraries
  for f in `ls "$YCSB_HOME"/core/target/*.jar`; do
    CLASSPATH="$CLASSPATH:$f"
  done

  # Database conf (need to find because location is not consistent)
  for f in `find "$YCSB_HOME"/$BINDING_DIR -name "conf"`; do
    CLASSPATH="$CLASSPATH:$f"
  done

  # Database libraries
  for f in `ls "$YCSB_HOME"/$BINDING_DIR/target/*.jar`; do
    CLASSPATH="$CLASSPATH:$f"
  done

  # Database dependency libraries
  for f in `ls "$YCSB_HOME"/$BINDING_DIR/target/dependency/*.jar 2>/dev/null`; do
    CLASSPATH="$CLASSPATH:$f"
  done
fi

# Cassandra deprecation message
if [ $BINDING_DIR = "cassandra" ] ; then
  echo "[WARN] The 'cassandra-7', 'cassandra-8', 'cassandra-10', and \
cassandra-cql' clients are deprecated. If you are using \
Cassandra 2.X try using the 'cassandra2-cql' client instead."
fi

# For Cygwin, switch paths to Windows format before running java
if $CYGWIN; then
  JAVA_HOME=`cygpath --absolute --windows "$JAVA_HOME"`
  CLASSPATH=`cygpath --path --windows "$CLASSPATH"`
fi

# Get the rest of the arguments
YCSB_ARGS=`echo $* | cut -d' ' -f3-`

# Run YCSB
echo \"$JAVA_HOME/bin/java\" $JAVA_OPTS -classpath \"$CLASSPATH\" $YCSB_CLASS $YCSB_COMMAND -db $BINDING_CLASS $YCSB_ARGS
"$JAVA_HOME/bin/java" $JAVA_OPTS -classpath "$CLASSPATH" $YCSB_CLASS $YCSB_COMMAND -db $BINDING_CLASS $YCSB_ARGS
