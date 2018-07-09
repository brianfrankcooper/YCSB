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
#        www.shellcheck.net was used to validate this script

# Cygwin support
CYGWIN=false
case "$(uname)" in
CYGWIN*) CYGWIN=true;;
esac

# Get script path
SCRIPT_DIR=$(dirname "$0" 2>/dev/null)

# Only set YCSB_HOME if not already set
[ -z "$YCSB_HOME" ] && YCSB_HOME=$(cd "$SCRIPT_DIR/.." || exit; pwd)

# Ensure that any extra CLASSPATH variables are set via setenv.sh
CLASSPATH=

# Pull in customization options
if [ -r "$YCSB_HOME/bin/setenv.sh" ]; then
  # Shellcheck wants a source, but this directive only runs if available
  #   So, tell shellcheck to ignore
  # shellcheck source=/dev/null
  . "$YCSB_HOME/bin/setenv.sh"
fi

# Attempt to find the available JAVA, if JAVA_HOME not set
if [ -z "$JAVA_HOME" ]; then
  JAVA_PATH=$(which java 2>/dev/null)
  if [ "x$JAVA_PATH" != "x" ]; then
    JAVA_HOME=$(dirname "$(dirname "$JAVA_PATH" 2>/dev/null)")
  fi
fi

# If JAVA_HOME still not set, error
if [ -z "$JAVA_HOME" ]; then
  echo "[ERROR] Java executable not found. Exiting."
  exit 1;
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
BINDING_LINE=$(grep "^$2:" "$YCSB_HOME/bin/bindings.properties" -m 1)

if [ -z "$BINDING_LINE" ] ; then
  echo "[ERROR] The specified binding '$2' was not found.  Exiting."
  exit 1;
fi

# Get binding name and class
BINDING_NAME=$(echo "$BINDING_LINE" | cut -d':' -f1)
BINDING_CLASS=$(echo "$BINDING_LINE" | cut -d':' -f2)

# Some bindings have multiple versions that are managed in the same directory.
#   They are noted with a '-' after the binding name.
#   (e.g. cassandra-7 & cassandra-8)
BINDING_DIR=$(echo "$BINDING_NAME" | cut -d'-' -f1)

# The 'basic' binding is core functionality
if [ "$BINDING_NAME" = "basic" ] ; then
  BINDING_DIR=core
fi

# For Cygwin, ensure paths are in UNIX format before anything is touched
if $CYGWIN; then
  [ -n "$JAVA_HOME" ] && JAVA_HOME=$(cygpath --unix "$JAVA_HOME")
  [ -n "$CLASSPATH" ] && CLASSPATH=$(cygpath --path --unix "$CLASSPATH")
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

# Accumulo deprecation message
if [ "${BINDING_DIR}" = "accumulo" ] ; then
  echo "[WARN] The 'accumulo' client has been deprecated in favor of version \
specific bindings. This name still maps to the binding for \
Accumulo 1.6, which is named 'accumulo-1.6'. This alias will \
be removed in a future YCSB release."
  BINDING_DIR="accumulo1.6"
fi

# Accumulo 1.6 deprecation message
if [ "${BINDING_DIR}" = "accumulo1.6" ] ; then
  echo "[WARN] The 'accumulo' client has been deprecated because Accumulo 1.6 \
is EOM. If you are using Accumulo 1.7+ try using the 'accumulo1.7' client \
instead."
fi


# Cassandra2 deprecation message
if [ "${BINDING_DIR}" = "cassandra2" ] ; then
  echo "[WARN] The 'cassandra2-cql' client has been deprecated. It has been \
renamed to simply 'cassandra-cql'. This alias will be removed  in the next \
YCSB release."
  BINDING_DIR="cassandra"
fi

# arangodb3 deprecation message
if [ "${BINDING_DIR}" = "arangodb3" ] ; then
  echo "[WARN] The 'arangodb3' client has been deprecated. The binding 'arangodb' \
now covers every ArangoDB version. This alias will be removed \
in the next YCSB release."
  BINDING_DIR="arangodb"
fi

# Build classpath
#   The "if" check after the "for" is because glob may just return the pattern
#   when no files are found.  The "if" makes sure the file is really there.
if $DISTRIBUTION; then
  # Core libraries
  for f in "$YCSB_HOME"/lib/*.jar ; do
    if [ -r "$f" ] ; then
      CLASSPATH="$CLASSPATH:$f"
    fi
  done

  # Database conf dir
  if [ -r "$YCSB_HOME"/"$BINDING_DIR"-binding/conf ] ; then
    CLASSPATH="$CLASSPATH:$YCSB_HOME/$BINDING_DIR-binding/conf"
  fi

  # Database libraries
  for f in "$YCSB_HOME"/"$BINDING_DIR"-binding/lib/*.jar ; do
    if [ -r "$f" ] ; then
      CLASSPATH="$CLASSPATH:$f"
    fi
  done

# Source checkout
else
  # Check for some basic libraries to see if the source has been built.
  if ! ls "$YCSB_HOME"/core/target/*.jar 1> /dev/null 2>&1 || \
     ! ls "$YCSB_HOME"/"$BINDING_DIR"/target/*.jar 1>/dev/null 2>&1; then
    # Call mvn to build source checkout.
    if [ "$BINDING_NAME" = "basic" ] ; then
      MVN_PROJECT=core
    else
      MVN_PROJECT="$BINDING_DIR"-binding
    fi

    echo "[WARN] YCSB libraries not found.  Attempting to build..."
    if mvn -Psource-run -pl com.yahoo.ycsb:"$MVN_PROJECT" -am package -DskipTests; then
      echo "[ERROR] Error trying to build project. Exiting."
      exit 1;
    fi
  fi

  # Core libraries
  for f in "$YCSB_HOME"/core/target/*.jar ; do
    if [ -r "$f" ] ; then
      CLASSPATH="$CLASSPATH:$f"
    fi
  done

  # Core dependency libraries
  for f in "$YCSB_HOME"/core/target/dependency/*.jar ; do
    if [ -r "$f" ] ; then
      CLASSPATH="$CLASSPATH:$f"
    fi
  done

  # Database conf (need to find because location is not consistent)
  CLASSPATH_CONF=$(find "$YCSB_HOME"/$BINDING_DIR -name "conf" | while IFS="" read -r file; do echo ":$file"; done)
  if [ "x$CLASSPATH_CONF" != "x" ]; then
    CLASSPATH="$CLASSPATH$CLASSPATH_CONF"
  fi

  # Database libraries
  for f in "$YCSB_HOME"/"$BINDING_DIR"/target/*.jar ; do
    if [ -r "$f" ] ; then
      CLASSPATH="$CLASSPATH:$f"
    fi
  done

  # Database dependency libraries
  for f in "$YCSB_HOME"/"$BINDING_DIR"/target/dependency/*.jar ; do
    if [ -r "$f" ] ; then
      CLASSPATH="$CLASSPATH:$f"
    fi
  done
fi

# Couchbase deprecation message
if [ "${BINDING_DIR}" = "couchbase" ] ; then
  echo "[WARN] The 'couchbase' client is deprecated. If you are using \
Couchbase 4.0+ try using the 'couchbase2' client instead."
fi

# HBase 0.98 deprecation message
if [ "${BINDING_DIR}" = "hbase098" ] ; then
  echo "[WARN] The 'hbase098' client is deprecated because HBase 0.98 \
is EOM. If you are using HBase 1.2+ try using the 'hbase12' client \
instead."
fi

# HBase 1.0 deprecation message
if [ "${BINDING_DIR}" = "hbase10" ] ; then
  echo "[WARN] The 'hbase10' client is deprecated because HBase 1.0 \
is EOM. If you are using HBase 1.2+ try using the 'hbase12' client \
instead."
fi

# For Cygwin, switch paths to Windows format before running java
if $CYGWIN; then
  [ -n "$JAVA_HOME" ] && JAVA_HOME=$(cygpath --unix "$JAVA_HOME")
  [ -n "$CLASSPATH" ] && CLASSPATH=$(cygpath --path --windows "$CLASSPATH")
fi

# Get the rest of the arguments
YCSB_ARGS=$(echo "$@" | cut -d' ' -f3-)

# About to run YCSB
echo "$JAVA_HOME/bin/java $JAVA_OPTS -classpath $CLASSPATH $YCSB_CLASS $YCSB_COMMAND -db $BINDING_CLASS $YCSB_ARGS"

# Run YCSB
# Shellcheck reports the following line as needing double quotes to prevent
# globbing and word splitting.  However, word splitting is the desired effect
# here.  So, the shellcheck error is disabled for this line.
# shellcheck disable=SC2086
"$JAVA_HOME/bin/java" $JAVA_OPTS -classpath "$CLASSPATH" $YCSB_CLASS $YCSB_COMMAND -db $BINDING_CLASS $YCSB_ARGS

