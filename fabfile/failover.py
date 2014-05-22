import sys, os
from datetime import timedelta
from re import search, compile

from conf import hosts
from conf import workloads

from fabfile.helpers import get_db, get_workload, _at, base_time, almost_nothing, \
                get_outfilename, get_properties, almost_nothing, base_time

from fabric import tasks
from fabric.colors import green
from fabric.context_managers import cd
from fabric.network import disconnect_all
#from fabric.operations import run, put
from fabric.operations import run, put, sudo

from pytz import timezone


# remote citrusleaf machines
tz = hosts.timezone
clients = hosts.env.roledefs['client']
servers = hosts.env.roledefs['server']
# benchmark file name, it bothers the CPU and consumes time and energy
benchmark_script = 'execute.sh'

# use LOCAL=True for testing on the local virtual machines
LOCAL = False
if LOCAL:
    # local virtual machines
    hosts.env.user = 'vagrant'
    hosts.env.password = 'vagrant'
    tz = timezone('CET')
#    clients = ['192.168.0.11', '192.168.0.12', '192.168.0.13', '192.168.0.14']
#    servers = ['192.168.0.10']
    clients = ['192.168.8.108', '192.168.9.213', '192.168.8.41', '192.168.8.118']
    servers = ['192.168.8.229']

#clients = [clients[0]]

def prepare_ycsbruncmd(the_hosts, dir_name, database, workload, the_time, target):
    # /opt/ycsb/bin/ycsb run couchbase ... -target 25000
    # and we assign
    # $1 -> run couchbase -s -P /opt/ycsb/workloads/workloada -p couchbase.user= -p couchbase.bucket=test -p couchbase.opTimeout=60000 -p couchbase.checkOperationStatus=true -p couchbase.password= -p couchbase.hosts=e1.citrusleaf.local,e2.citrusleaf.local,e3.citrusleaf.local,e4.citrusleaf.local -p fieldnameprefix=f -p recordcount=50000000 -p fieldcount=10 -p retrydelay=1 -p threadcount=32 -p readretrycount=1000 -p fieldlength=10 -p exportmeasurementsinterval=30000 -p workload=com.yahoo.ycsb.workloads.CoreWorkload -p updateretrycount=1000 -p insertretrycount=1000000 -p warmupexecutiontime=60000 -p operationcount=2500000
    # $2 -> -target 25000
    # $2 could be empty
    par = '' # /opt/ycsb/bin/ycsb is hardcoded in the benchmark file
    par += ' run %s -s' % database['command']
    for file in workload['propertyfiles']:
        par += ' -P %s' % file
    for (key, value) in get_properties(database, workload).items():
        par += ' -p %s=%s' % (key, value)
    for (key, value) in workloads.data.items():
        if key == 'operationcount':
            par += ' -p %s=%s' % (key, int(value) / len(the_hosts))
        else:
            par += ' -p %s=%s' % (key, value)
    if target is not None:
        par += ' -target %s' % str(target)
        # parameters are constructed
    outfile = get_outfilename(database['name'], workload['name'], 'out', the_time, target)
    errfile = get_outfilename(database['name'], workload['name'], 'err', the_time, target)
    cmd = './%s %s' % (benchmark_script, par)
    cmd += ' > %s/%s' % (dir_name, outfile)
    cmd += ' 2> %s/%s' % (dir_name, errfile)
    return cmd

def prepare_killcmd(database):
    return database['failover']['kill_command']

def prepare_startcmd(database):
    return database['failover']['start_command']

def initialize(the_hosts, db):
    """
    Prepares hosts to run the series
    """
    database = get_db(db)
    db_home = database['home']
    pf = compile('^%s' % database['name'])
    pn = compile('(\d+)/$')
    nos = [0]

    def inner_initialize_0():
    #    sudo('yum -y install at')
    #    sudo('service atd start')
    #    sudo('sudo yum install -y java-1.7.0-openjdk-devel')
    #    with cd('/opt'):
    #        put('../distribution/target/ycsb-0.1.4.tar.gz', '/run/shm/ycsb.tar.gz')
    #        sudo('rm -r ycsb-0.1.4')
    #        sudo('tar xzvf /run/shm/ycsb.tar.gz')
    #        sudo('ln -s /opt/ycsb-0.1.4 /opt/ycsb')
    #        print 'ycsb deployed'
        sudo('mkdir -p %s ; chmod 1777 %s' % (db_home, db_home))
        with cd(db_home):
            ls = run("ls --format=single-column --sort=t -d -- */")
            # the most recent file satisfying pattern
            if ls:
                ls = ls.split('\r\n')
                file_names = [f for f in ls if pf.search(f)]
                for file_name in file_names:
                    mn = pn.search(file_name)
                    if mn:
                        nos.append(int(mn.group(1)) + 1)
        # find the maximum number for all of the hosts
    with almost_nothing():
        tasks.execute(inner_initialize_0, hosts=the_hosts)
        # now form the dir name
    dir_name = os.path.join(database['home'], '%s_%02d' % (database['name'], max(nos)))
    def inner_initialize_1():
        run('mkdir %s ' % dir_name)
        series_dir = os.path.dirname(__file__)
        local_benchmark_script = os.path.join(series_dir, benchmark_script)
        if LOCAL:
            with cd(dir_name):
                run('rm -rf ./*')
                put(local_benchmark_script, benchmark_script, mode=0744)
            #                run('sed -i "s/\/opt\/ycsb\/bin\/ycsb \$\*/python nbody.py \$\*/g" %s' % benchmark_script)
        else:
            # if not LOCAL
            with cd(dir_name):
                put(local_benchmark_script, benchmark_script, mode=0744)

        # continue init
        # clear all the tasks that submitted so far
        with cd(dir_name):
            tasks = run('atq').split('\r\n')
            tid = []
            for task in tasks:
                m = search('^(\d+)\t', task)
                if m:
                    tid.append(m.group(1))
            if tid:
                run('atrm %s' % ' '.join(tid))
            print green('host %s initialized ' % hosts.env.host)

    with almost_nothing():
        tasks.execute(inner_initialize_1, hosts=the_hosts)

    return dir_name

def initialize_servers(db):
    """
    Prepares server hosts to run the failover test
    """
    database = get_db(db)
    local_dir = os.path.dirname(__file__)
    def inner_initialize():
        for file in database['failover']['files']:
            put(os.path.join(local_dir, file), file, mode=0744)
        print green('host %s initialized ' % hosts.env.host)
    with almost_nothing():
        tasks.execute(inner_initialize, hosts=servers)

def submit_workload(the_hosts, dir_name, db, workload, the_time, target = None):
    """
    Schedules the workload.
    Note: we cannot use ycsb.workload, because it is decorated
    """
    database = get_db(db)
    load = get_workload(workload)
    def inner_submit_workload():
        with cd(dir_name):
            param = int(target) / len(the_hosts) if target is not None else None
            # command = prepare_ycsbruncmd(database, load, the_time, param)
            command = _at(prepare_ycsbruncmd(the_hosts, dir_name, database, load, the_time, param), the_time)
            run(command)

    with almost_nothing():
        tasks.execute(inner_submit_workload, hosts=the_hosts)


def delay(wl, t):
    """ Returns estimated delay (run time) for the test with parameter t.
    In seconds """
    opc = workloads.data['operationcount']
    # redefine operation count if the workload hath
    workload = get_workload(wl)
    if 'properties' in workload:
        if 'operationcount' in workload['properties']:
            opc = long(workload['properties']['operationcount'])
    t = opc if t is None else t
    d = int((opc / t) * 1.1)
    return timedelta(seconds = d)

def run_test_series(db, seq):
    """ This script takes a sequence of threshold values and executes tests """
    dir_name = initialize(clients, db)
    the_time = base_time(tz = tz)
    for (wl, t) in seq:
        t = t if t > 0 else None
        # submit the task
        submit_workload(clients, dir_name, db, wl, the_time, t)
        print green("submitted on %s with threshold = %s" % (the_time, t))
        if LOCAL:
            the_time += timedelta(minutes = 1)
        else:
            the_time += delay(wl, t)
            the_time = base_time(the_time, tz = tz) # round the time up
        # end of all
    disconnect_all()

######################################
# Remote actions hierarchy goes here #
######################################

class RemoteBase:
    def __init__(self, hosts, time, tz, dir_name):
        # remote hosts to be executed on
        self.hosts = hosts
        self.time = time
        self.tz = tz
        self.dir_name = dir_name
    def delay_after(self):
        return self.time
    def call(self):
        print green("called %s at %s for %s" % (self, self.time, self.hosts))

class RemoteInit(RemoteBase):
    def __init__(self, db, *base):
        RemoteBase.__init__(self, *base)


class RemoteRun(RemoteBase):
    def __init__(self, db, wl, dir_path, thr, *base):
        RemoteBase.__init__(self, *base)
        self.db = db
        self.wl = wl
        self.dir_path = dir_path
        self.thr = thr
    def delay_after(self):
        """ Returns estimated delay (run time) for the test with parameter t.
        In seconds """
        opc = workloads.data['operationcount']
        # redefine operation count if the workload hath
        workload = get_workload(self.wl)
        if 'properties' in workload:
            if 'operationcount' in workload['properties']:
                opc = long(workload['properties']['operationcount'])
        t = opc if self.thr is None else self.thr
        d = int((opc / t) * 1.1)
        the_time = self.time + timedelta(seconds = d)
        the_time = base_time(the_time, tz = self.tz) # round the time up
        return the_time - self.time

    def call(self):
        submit_workload(clients, self.dir_path, self.db, self.wl, self.time, self.thr)
        print green("at %s submitted run with threshold = %s (%s)" % (self.time, self.thr, self.hosts))


class RemoteKill(RemoteBase):
    def __init__(self, db, *base):
        RemoteBase.__init__(self, *base)
        self.db = db
    def call(self):
        database = get_db(self.db)
        the_time = self.time
        def inner():
            command = _at(prepare_killcmd(database), the_time)
            run(command, shell=True)
        with almost_nothing():
            tasks.execute(inner, hosts=self.hosts)
        print green("at %s submitted server kill (%s)" % (self.time, self.hosts))

class RemoteStart(RemoteBase):
    def __init__(self, db, *base):
        RemoteBase.__init__(self, *base)
        self.db = db
    def call(self):
        database = get_db(self.db)
        the_time = self.time
        def inner():
            command = _at(prepare_startcmd(database), the_time)
            run(command, shell=True)
        with almost_nothing():
            tasks.execute(inner, hosts=self.hosts)
        print green("at %s submitted server start (%s)" % (self.time, self.hosts))

class RemoteNetworkUp(RemoteBase):
    def __init__(self, *base):
        RemoteBase.__init__(self, *base)
    def call(self):
        print green("at %s submitted network up (%s)" % (self.time, self.hosts))

class RemoteNetworkDown(RemoteBase):
    def __init__(self, *base):
        RemoteBase.__init__(self, *base)
    def call(self):
        print green("at %s submitted network down (%s)" % (self.time, self.hosts))


class Network:
    UP = 1
    DOWN = 2

class Launcher:
    def __init__(self, at, delta):
        self.at = at
        self.delta = delta

    def _common(self, ctor, hosts, *args):
        delta = self.at.base_time + self.delta
        tz = self.at.tz
        dir_name = self.at.dir_name
        ext_args = list(args) + [hosts, delta, tz, dir_name]
        step = ctor(*ext_args)
        self.at.seq.append(step)
        return self.delta + step.delay_after()

    def client_run(self, hosts, db, wl, thr = None):
        dn = self.at.dir_name
        return self._common(RemoteRun, hosts, db, wl, dn, thr)

    def server_kill(self, hosts, db):
        return self._common(RemoteKill, hosts, db)

    def server_start(self, hosts, db):
        return self._common(RemoteStart, hosts, db)

    def server_network(self, hosts, network_flag):
        if network_flag == Network.UP:
            nw_ctor = RemoteNetworkUp
        else:
            nw_ctor = RemoteNetworkDown
        return self._common(nw_ctor, hosts)



class AT:
    def __init__(self, the_db, the_tz = hosts.timezone):
        self.dir_name = initialize(clients, the_db)
        self.base_time = base_time(tz = the_tz)
        initialize_servers(the_db)
        self.tz = the_tz
        self.seq = []

    def __getitem__(self, delta):
        """
        :type delta: timedelta
        delta is the time span since self is constructed
        """
        if type(delta) == int:
            delta = timedelta(seconds = delta)
        return Launcher(self, delta)

    def fire(self):
        # self.the_seq is the sequence of remote actions
        # ra is abbreviation of RemoteBase
        for rb in self.seq:
            if isinstance(rb, RemoteBase):
                rb.call()
        # end of all
        disconnect_all()

