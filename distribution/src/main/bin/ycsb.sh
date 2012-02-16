#! /usr/bin/env bash

# Set the YCSB specific environment. Adds all the required libraries to the class path.

# Copyright (c) 2010 Yahoo! Inc. All rights reserved.                                                                                                                             
# *                                                                                                                                                                                 
# * Licensed under the Apache License, Version 2.0 (the "License"); you                                                                                                             
# * may not use this file except in compliance with the License. You                                                                                                                
# * may obtain a copy of the License at                                                                                                                                             
# *                                                                                                                                                                                 
# * http://www.apache.org/licenses/LICENSE-2.0                                                                                                                                      
# *                                                                                                                                                                                 
# * Unless required by applicable law or agreed to in writing, software                                                                                                             
# * distributed under the License is distributed on an "AS IS" BASIS,                                                                                                               
# * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or                                                                                                                 
# * implied. See the License for the specific language governing                                                                                                                    
# * permissions and limitations under the License. See accompanying                                                                                                                 
# * LICENSE file. 
#

# The Java implementation to use. This is required.
#export JAVA_HOME=

# Any JVM options to pass.
#export YCSB_OPTS="-Djava.compiler=NONE"

# YCSB client heap size.
#export YCSB_HEAP_SIZE=500

this=`dirname "$0"`
this=`cd "$this"; pwd`

while [ -h "$this" ]; do
  ls=`ls -ld "$this"`
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '.*/.*' > /dev/null; then
    this="$link"
  else
    this=`dirname "$this"`/"$link"
  fi
done

bin=`dirname "$this"`
script=`basename "$this"`
bin=`cd "$bin"; pwd`
this="$bin/$script"

# the root of the Hadoop installation
export YCSB_HOME=`dirname "$this"`

echo "YCSB_HOME $YCSB_HOME"

cygwin=false
case "`uname`" in
CYGWIN*) cygwin=true;;
esac

# if no args specified, show usage
if [ $# = 0 ]; then
  echo "Usage: ycsb CLASSNAME"
  echo "where CLASSNAME is the name of the class to run"
  echo "The jar file for the class must be in bin, build, lib, or db/*/lib."
  exit 1
fi

# get arguments
COMMAND=$1
shift

JAVA=""
if [ "$JAVA_HOME" != "" ]; then
  JAVA=$JAVA_HOME/bin/java
else
  echo "JAVA_HOME must be set."
  exit 1
fi

JAVA_HEAP_MAX=-Xmx500m
# check envvars which might override default args
if [ "$YCSB_HEAP_SIZE" != "" ]; then
  JAVA_HEAP_MAX="-Xmx""$YCSB_HEAP_SIZE""m"
fi

# Set the classpath.

if [ "$CLASSPATH" != "" ]; then
  CLASSPATH=${CLASSPATH}:$JAVA_HOME/lib/tools.jar
else
  CLASSPATH=$JAVA_HOME/lib/tools.jar
fi

# so that filenames w/ spaces are handled correctly in loops below
IFS=

for f in $YCSB_HOME/build/*.jar; do
  CLASSPATH=${CLASSPATH}:$f
done

for f in $YCSB_HOME/lib/*.jar; do
  CLASSPATH=${CLASSPATH}:$f
done

for f in $YCSB_HOME/db/*; do
  if [ -d $f ]; then
    for j in $f/lib/*.jar; do
      CLASSPATH=${CLASSPATH}:$j
    done
  fi
done

#echo "CLASSPATH=$CLASSPATH"

# restore ordinary behavior
unset IFS

CLASS=$COMMAND

# cygwin path translation
if $cygwin; then
  CLASSPATH=`cygpath -p -w "$CLASSPATH"`
  YCSB_HOME=`cygpath -w "$YCSB_HOME"`
fi

#echo "Executing command $CLASS with options $JAVA_HEAP_MAX $YCSB_OPTS $CLASS $@"
exec "$JAVA" $JAVA_HEAP_MAX $YCSB_OPTS -classpath "$CLASSPATH" $CLASS "$@"
