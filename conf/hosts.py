from fabric.api import env
import pytz

#user name to ssh to hosts
env.user = 'root'
#user password (the better is to use pubkey authentication)
env.password = 'password'

env.show = ['debug']

env.roledefs = {
    #list of client hosts
    'client': ['c1.local', 'c2.local'],
    #'client': ['c1.local', 'c2.local', 'c3.local', 'c4.local'],
    #'client': ['c1.local', 'c2.local', 'c3.local', 'c4.local', 'c5.local', 'c6.local'],
    #'client': ['c1.local', 'c2.local', 'c3.local', 'c4.local', 'r1.local', 'r2.local', 'r3.local', 'r5.local'],
    #'client': ['c1.local', 'c2.local', 'c3.local', 'c4.local', 'c5.local', 'c6.local', 'r1.local', 'r2.local', 'r3.local', 'r5.local'],

    #list of server hosts
    'server': ['e1.local', 'e2.local', 'e3.local', 'e4.local'],

    #list of all available client hosts
    'all_client': ['c1.local', 'c2.local', 'c3.local', 'c4.local', 'c5.local', 'c6.local', 'r1.local', 'r2.local', 'r3.local', 'r5.local'],
}

#hosts timezone (required to correctly schedule ycsb tasks)
timezone = pytz.timezone('US/Pacific')
