root = '/opt/ycsb'  #root of YCSB installation

data = {    #global YSCB properties
    'recordcount': 200000000,  #SSD
    #'recordcount': 50000000,    #RAM
    'fieldcount': 10,
    'fieldlength': 10,
    'fieldnameprefix': 'f',
    #'operationcount': 10000000,
    'operationcount': 200000000,    #>10min for Aerospike and Couchbase
    #'operationcount': 50000000,    #>10min for Cassandra
    'threadcount': 32,
    'workload': 'com.yahoo.ycsb.workloads.CoreWorkload',
    'exportmeasurementsinterval': 30000,
    #'warmupexecutiontime': 60000,
    'insertretrycount': 1000000000,
    'readretrycount': 1000,
    'updateretrycount': 1000,
    'retrydelay': 1,
    #'readallfields': 'false',
    #'writeallfields': 'false',
}

workloads = {
    'A': {  #Heavy Update workload
        'name': 'workloada',    #name of the workload to be part of the log files
        'propertyfiles': [ root + '/workloads/workloada' ], #workload properties files
    },
    'B': {  #Mostly Read workload
        'name': 'workloadb',
        'propertyfiles': [ root + '/workloads/workloadb' ],
    },
    'C': {  #Read Only workload
        'name': 'workloadc',
        'propertyfiles': [ root + '/workloads/workloadc' ],
        'properties': {     #additional workload properties, overrides the global ones
            'maxexecutiontime': 60000,
        },
    },
}
