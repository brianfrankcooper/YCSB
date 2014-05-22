#!/usr/bin/python
import sys, os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..')) # ugly hack to allow import from the root
from fabfile.helpers import _at
from conf.databases import databases

print _at(databases['couchbase2']['failover']['kill_command']);
print _at(databases['couchbase2']['failover']['start_command']);
