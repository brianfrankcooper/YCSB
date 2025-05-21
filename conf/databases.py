import hosts

databases = {

    'aerospike' : {
        'name': 'aerospike',    #name of the database (used to form the logfile name)
        'home': '/run/shm',     #database home, to put logs there
        'command': 'aerospike', #database name to pass to ycsb command
        'properties': {         #properties to pass to ycsb command as -p name=value
            'host': 'e1.local',  #database connection params
            'port': 3000,
            'ns': 'test',
            'set': 'YCSB',
        },
        'status': {
            'hosts': hosts.env.roledefs['server'][0:1],     #hosts on which to run the status command
            'command': '/opt/citrusleaf/bin/clmonitor -e info'  #the status command
        }
    },

    'couchbase' : {
        'name': 'couchbase',
        'home': '/run/shm',
        'command': 'couchbase',
        'properties': {
            'couchbase.hosts': 'e1.local,e2.local,e3.local,e4.local',
            'couchbase.bucket': 'test',
            'couchbase.user': '',
            'couchbase.password': '',
            'couchbase.opTimeout': 60000,
            #'couchbase.failureMode': 'Retry',
            'couchbase.checkOperationStatus': 'true',
        }
    },

    'couchbase2' : {
        'name': 'couchbase',
        'home': '/run/shm',
        'command': 'couchbase2',
        'properties': {
            'couchbase.hosts': 'e1.local,e2.local,e3.local,e4.local',
            'couchbase.bucket': 'test',
            'couchbase.ddocs': '',
            'couchbase.views': '',
            'couchbase.user': '',
            'couchbase.password': '',
            'couchbase.opTimeout': 60000,
            #'couchbase.failureMode': 'Retry',
            'couchbase.persistTo': 'ZERO',
            'couchbase.replicateTo': 'ZERO',
            'couchbase.checkOperationStatus': 'true',
            }
    },

    'cassandra' : {
        'name': 'cassandra',
        'home': '/run/shm',
        'command': 'cassandra-10',
        'properties': {
            'hosts': 'e1.local,e2.local,e3.local,e4.local',
        }
    },

    'mongodb' : {
        'name': 'mongodb',
        'home': '/run/shm',
        'command': 'mongodb',
        'properties': {
            'mongodb.url': 'mongodb://localhost:27017',
            'mongodb.database': 'ycsb',
            'mongodb.writeConcern': 'normal',
            'mongodb.readPreference': 'primaryPreferred',
        },
        'configdb': 'r5.citrusleaf.local',
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
