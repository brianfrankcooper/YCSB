#!/bin/sh

/bin/kill -9 $(/bin/cat /opt/couchbase/var/lib/couchbase/couchbase-server.pid)
