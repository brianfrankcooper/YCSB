from urlparse import urlparse

from fabric.api import *

from conf.databases import databases
from conf import hosts

@roles('all_client')
@parallel
def mongos_restart():
    """Restarts mongos on all clients"""
    url = urlparse(databases['mongodb']['properties']['mongodb.url'])
    configdb = databases['mongodb']['configdb']
    with settings(warn_only=True):
        run('killall mongos')
        run('/opt/mongodb/bin/mongos --configdb %s --port %d --logpath mongos.log --logappend --quiet --fork' % (configdb, url.port))

@roles('all_client')
@parallel
def mongos_stop():
    """Stops mongos on all clients"""
    with settings(warn_only=True):
        run('killall mongos')
