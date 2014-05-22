#!/usr/bin/python
from datetime import timedelta
import sys, os
import pytz

sys.path.insert(0, os.path.abspath('..')) # ugly hack to allow import from the root
from fabfile.failover import clients, servers, AT, Launcher, Network


#########################
c1, c2, c3, c4 = clients
e1, e2, e3, e4 = servers
#e1 = '192.168.0.10'
#tz = pytz.timezone('Asia/Omsk')
db = 'basic'
wl = 'C'

at = AT(clients, db)
one_min = timedelta(minutes = 1)
# run clients bombarding the servers with requests
t0 = at[0].client_run([c1, c2], db, wl) + one_min
t0 = at[t0].client_run([c1, c2], db, wl, 50000)
t0 = at[t0].client_run([c1, c2], db, wl, 50000)
t0 = at[t0].client_run([c1, c2], db, wl, 50000)
# in 1 minute (=60 sec) the first server fails
at[60].server_kill([e1], db)
# in 2 minutes the server is up back
at[120].server_start([e1], db)

at[150].server_network([e1], Network.DOWN)
at[151].server_network([e1], Network.UP)

# test again, to verify if the previous failure influenced behavior
t1 = at[t0].client_run([c1, c2], db, wl)

at.fire()
