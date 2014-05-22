from fabric.api import env
import pytz

#user name to ssh to hosts
env.user = 'root'
#user password (the better is to use pubkey authentication)
env.password = 'thumbtack'

env.show = ['debug']

env.roledefs = {
    #list of client hosts
    #'client': ['c1.citrusleaf.local',],
    'client': ['c1.citrusleaf.local', 'c2.citrusleaf.local', 'c3.citrusleaf.local', 'c4.citrusleaf.local'],
    #'client': ['c1.citrusleaf.local', 'c2.citrusleaf.local', 'c3.citrusleaf.local', 'c4.citrusleaf.local', 'c5.citrusleaf.local', 'c6.citrusleaf.local'],
    #'client': ['c1.citrusleaf.local', 'c2.citrusleaf.local', 'c3.citrusleaf.local', 'c6.citrusleaf.local', 'r1.citrusleaf.local', 'r2.citrusleaf.local', 'r3.citrusleaf.local', 'r5.citrusleaf.local'],
    #'client': ['c1.citrusleaf.local', 'c2.citrusleaf.local', 'c3.citrusleaf.local', 'c6.citrusleaf.local', 'c4.citrusleaf.local', 'c5.citrusleaf.local', 'r3.citrusleaf.local', 'r5.citrusleaf.local'],
    #'client': ['c1.citrusleaf.local', 'c2.citrusleaf.local', 'c3.citrusleaf.local', 'c4.citrusleaf.local', 'c5.citrusleaf.local', 'c6.citrusleaf.local', 'r1.citrusleaf.local', 'r2.citrusleaf.local', 'r3.citrusleaf.local', 'r5.citrusleaf.local'],

    #list of server hosts
    'server': ['e1.citrusleaf.local', 'e2.citrusleaf.local', 'e3.citrusleaf.local', 'e4.citrusleaf.local'],

    #list of all available client hosts
    'all_client': ['c1.citrusleaf.local', 'c2.citrusleaf.local', 'c3.citrusleaf.local', 'c4.citrusleaf.local', 'c5.citrusleaf.local', 'c6.citrusleaf.local', 'r1.citrusleaf.local', 'r2.citrusleaf.local', 'r3.citrusleaf.local', 'r5.citrusleaf.local'],
    #'all_client': ['c1.citrusleaf.local', 'c2.citrusleaf.local', 'c3.citrusleaf.local', 'c6.citrusleaf.local', 'r1.citrusleaf.local', 'r2.citrusleaf.local', 'r3.citrusleaf.local', 'r5.citrusleaf.local'],
}

#hosts timezone (required to correctly schedule ycsb tasks)
timezone = pytz.timezone('US/Pacific')
