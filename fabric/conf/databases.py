import hosts

databases = {

    'aerospike' : {
        'name': 'aerospike',    #name of the database (used to form the logfile name)
        'home': '/run/shm',     #database home, to put logs there
        'command': 'aerospike', #database name to pass to ycsb command
        'properties': {         #properties to pass to ycsb command as -p name=value
            'host': 'e1.citrusleaf.local',  #database connection params
            'port': 3000,
            'ns': 'test',
            'set': 'YCSB',
        },
        'status': {
            'hosts': hosts.env.roledefs['server'][0:1],     #hosts on which to run the status command
            'command': '/opt/citrusleaf/bin/clmonitor -e info'  #the status command
        },
        'failover': {
            'files': [],
            'kill_command': '/usr/bin/killall -9 cld',
            'start_command': '/etc/init.d/citrusleaf start',
        },
    },

    'couchbase' : {
        'name': 'couchbase',
        'home': '/run/shm',
        'command': 'couchbase',
        'properties': {
            'couchbase.hosts': 'e1.citrusleaf.local,e2.citrusleaf.local,e3.citrusleaf.local,e4.citrusleaf.local',
            'couchbase.bucket': 'test',
            'couchbase.user': '',
            'couchbase.password': '',
            'couchbase.opTimeout': 1000,
            #'couchbase.failureMode': 'Retry',
            'couchbase.checkOperationStatus': 'true',
        }
    },

    'couchbase2' : {
        'name': 'couchbase',
        'home': '/run/shm',
        'command': 'couchbase2',
        'properties': {
            'couchbase.hosts': 'e2.citrusleaf.local,e1.citrusleaf.local,e3.citrusleaf.local,e4.citrusleaf.local',
            'couchbase.bucket': 'test',
            'couchbase.ddocs': '',
            'couchbase.views': '',
            'couchbase.user': '',
            'couchbase.password': '',
            'couchbase.opTimeout': 1000,
            #'couchbase.failureMode': 'Retry',
            #'couchbase.persistTo': 'ONE',
            #'couchbase.replicateTo': 'ONE',
            'couchbase.checkOperationStatus': 'true',
            },
        'failover': {
            'files': ['couchbase_kill.sh', 'couchbase_start.sh'],
            'kill_command': '''\
ssh e1 ~/couchbase_kill.sh; \
sleep 1; \
/opt/couchbase/bin/couchbase-cli failover -c localhost:8091 -u admin -p 123123 --server-failover=192.168.109.168; \
sleep 2; \
/opt/couchbase/bin/couchbase-cli rebalance -c localhost:8091 -u admin -p 123123;''',

            'start_command': '''\
ssh e1 ~/couchbase_start.sh; \
sleep 7; \
/opt/couchbase/bin/couchbase-cli server-add -c localhost:8091 -u admin -p 123123 --server-add=192.168.109.168 --server-add-username=admin --server-add-password=123123; \
sleep 3; \
/opt/couchbase/bin/couchbase-cli rebalance -c localhost:8091 -u admin -p 123123;''',
        }
    },

    'cassandra' : {
        'name': 'cassandra',
        'home': '/root/ycsb',
        'command': 'cassandra-hector',
        'properties': {
            'hosts': 'e1.citrusleaf.local,e2.citrusleaf.local,e3.citrusleaf.local,e4.citrusleaf.local',
            'cassandra.readconsistencylevel': 'ONE',
            'cassandra.writeconsistencylevel': 'ONE', #ALL-sync/ONE-async
        },
        'failover': {
            'files': [],
            'kill_command': '/usr/bin/killall -9 java',
            'start_command': '/opt/cassandra/bin/cassandra',
        },
    },

    'mongodb' : {
        'name': 'mongodb',
        'home': '/root/ycsb',        
        #'home': '/run/shm',
        'command': 'mongodb',
        'properties': {
            'mongodb.url': 'mongodb://localhost:27018',
            'mongodb.database': 'ycsb',
            'mongodb.writeConcern': 'normal',
            #'mongodb.writeConcern': 'replicas_safe',
            'mongodb.readPreference': 'primaryPreferred',
        },
        'configdb': 'r5.citrusleaf.local',
        'failover': {
            'files': [],
            'kill_command': '/usr/bin/killall -9 mongod',
            'start_command': '~/mongo_run.sh',
        },
    },

    'hbase' : {
        'name': 'hbase',
        'home': '/run/shm',
        'command': 'hbase',
        'properties': {
            'columnfamily': 'family',
        }
    },


    'basic' : { #fake database
        'name': 'basic',
        'home': '/run/shm',
        'command': 'basic',
        'properties': {
            'basicdb.verbose': 'false',
        }
    },

}
