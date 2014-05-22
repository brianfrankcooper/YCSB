#!/usr/bin/python
from datetime import timedelta
import sys, os
import pytz

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..')) # ugly hack to allow import from the root
from fabfile.failover import clients, servers, AT


#########################
db = 'aerospike'

at = AT(db)

# kill server
at[0].server_kill(servers, db)

at.fire()
