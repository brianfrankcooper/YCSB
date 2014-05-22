import re, os, tempfile

from fabric.api import *
from fabric.colors import green, blue, red
from fabric.contrib.console import confirm

from conf import workloads
from fabfile.helpers import get_db, get_workload, _at, get_outfilename, base_time, almost_nothing, get_properties, determine_file

def _ycsbloadcmd(database, clientno, timestamp, target=None):
    totalclients = len(env.roledefs['client'])
    cmd = workloads.root + '/bin/ycsb load %s -s' % database['command']
    for (key, value) in get_properties(database).items():
        if key == 'operationcount':
            cmd += ' -p %s=%s' % (key, value / totalclients)
        else:
            cmd += ' -p %s=%s' % (key, value)
    insertcount = workloads.data['recordcount'] / totalclients
    insertstart = insertcount * clientno
    cmd += ' -p insertstart=%s' % insertstart
    cmd += ' -p insertcount=%s' % insertcount
    if target is not None:
        cmd += ' -target %s' % str(target)
    outfile = get_outfilename(database['name'], 'load', 'out', timestamp)
    errfile = get_outfilename(database['name'], 'load', 'err', timestamp)
    cmd += ' > %s/%s' % (database['home'], outfile)
    cmd += ' 2> %s/%s' % (database['home'], errfile)
    return cmd

def _ycsbruncmd(database, workload, timestamp, target=None):
    totalclients = len(env.roledefs['client'])
    cmd = workloads.root + '/bin/ycsb'
    cmd += ' run %s -s' % database['command']
    for file in workload['propertyfiles']:
        cmd += ' -P %s' % file
    for (key, value) in get_properties(database, workload).items():
        if key == 'operationcount':
            cmd += ' -p %s=%s' % (key, value / totalclients)
        else:
            cmd += ' -p %s=%s' % (key, value)
    if target is not None:
        cmd += ' -target %s' % str(target)
    outfile = get_outfilename(database['name'], workload['name'], 'out', timestamp, target)
    errfile = get_outfilename(database['name'], workload['name'], 'err', timestamp, target)
    cmd += ' > %s/%s' % (database['home'], outfile)
    cmd += ' 2> %s/%s' % (database['home'], errfile)
    return cmd

def _client_no():
    # env.all_hosts is empty for @parallel in fabric 1.3.2
    # return env.all_hosts.index(env.host)
    return env.roledefs['client'].index(env.host)

def _totalclients():
    return len(env.roledefs['client'])

@roles('client')
def load(db, target=None):
    """Starts loading of data to the database"""
    timestamp = base_time()
    print green(timestamp, bold = True)
    clientno = _client_no()
    database = get_db(db)
    with cd(database['home']):
        if target is not None:
            part = int(target) / len(env.roledefs['client'])
            run(_at(_ycsbloadcmd(database, clientno, timestamp, part), timestamp))
        else:
            run(_at(_ycsbloadcmd(database, clientno, timestamp), timestamp))

@roles('client')
def run_workload(db, workload, target=None):
    """Starts running of the workload"""
    timestamp = base_time()
    print green(timestamp, bold = True)
    database = get_db(db)
    load = get_workload(workload)
    with cd(database['home']):
        if target is not None:
            part = int(target) / len(env.roledefs['client'])
            run(_at(_ycsbruncmd(database, load, timestamp, part), timestamp))
        else:
            run(_at(_ycsbruncmd(database, load, timestamp), timestamp))

@roles('client')
def status(db):
    """ Shows status of the currently running YCSBs """
    with almost_nothing():
        database = get_db(db)
        dir_name = database['home']
        with cd(database['home']):
            # we need to provide the info for the latest modified
            # file, therefore we need to check the times
            # recursively list all err files and modification times
            lines = run('find . -name "*.err" -printf "%p %T@\n"').split('\r\n')
            def extract(line):
                (f, t) = line.split(' ')
                (d, f) = os.path.split(f)
                return (d, f, float(t))
            def order(t):
                return -t[2]
            if lines and not (len(lines) == 1 and not lines[0]):
                files = sorted(map(extract, lines), key = order)
                (d, f, t) = files[0]
                dir_name = os.path.normpath(os.path.join(database['home'], d))
        with cd(dir_name):
            ls_out = run('ls --format=single-column --sort=t *.lock')
            msg = green('free') if 'cannot access' in ls_out else red('locked')
            print blue('Lock:', bold = True), msg
            print blue('Scheduled:', bold = True)
            # print run('tail -n 2 /var/spool/cron/atjobs/*')
            print sudo('atq')
            print blue('Running:', bold = True)
            print run('ps -f -C java')
            # print run('ps aux | grep python')
            # sort the output of ls by date, the first entry should be the *.err needed
            ls_out = run('ls --format=single-column --sort=t *.err')
            if 'cannot access' in ls_out:
                logfile = '<no any *.err files>'
                tail = ''
            else:
                ls = ls_out.split("\r\n")
                logfile = ls[0]
                tail = run('tail %s' % logfile)
            print blue('Dir:', bold = True), green(dir_name, bold = True)
            print blue('Log:', bold = True), green(logfile, bold = True)
            print tail
            # print blue('List:', bold = True)
            # ls_out = run('ls --format=single-column --sort=t')
            # print ls_out
            print

@roles('client')
@parallel
def get_log(db, regex='.*', do=False):
    """ Download *.err and *.out logs satisfying the regex to be transferred
    OR transfer all logs in the batch dir
    """

    database = get_db(db)
    with almost_nothing():
        cn = _client_no() + 1
        with cd(database['home']):
            (f0, is_dir) = determine_file(regex)
            print blue('Filename at c%s: ' % cn, bold = True), green(f0, bold = True)
        # now do the processing, if enabled
        #If do is presented, then do is str. bool(str) = True if str is not empty
        #May be we should process str as bool?
        if do:
            with cd(database['home']):
                if is_dir:
                    tempdir_local = '%s/c%s' % (tempfile.gettempdir(), cn)
                    dir_local = './%s-c%s' % (f0, cn)
                    bz2_remote = '%s-c%s-dir.bz2' % (f0, cn)
                    bz2_full_local = '%s/%s-dir.bz2' % (tempdir_local, f0)
                    # packing
                    print blue('c%s packing ...' % cn)
                    #run('tar -jcvf %s %s' % (bz2_remote, f0))
                    #To ignore huge err files.
                    run('tar -jcvf %s %s/*.out' % (bz2_remote, f0))
                    # download them
                    print blue('c%s transferring to %s...' % (cn, tempdir_local))
                    get(bz2_remote, bz2_full_local)
                    # the files are here, remove remote bz2
                    run('rm -f %s' % bz2_remote)
                    # unpacking to temp dir
                    print blue('c%s unpacking ...' % cn)
                    local('tar -xvf %s -C %s' % (bz2_full_local, tempdir_local))
                    print blue('c%s moving to current dir ...' % cn)
                    # remove the old version of the dir
                    # TODO maybe use versioning?
                    local('rm -rf %s' % dir_local)
                    local('mkdir -p %s' % dir_local)
                    local('mv %s/%s/* %s' % (tempdir_local, f0, dir_local))
                    # additional step - rename all the files
                    # *.err and *.out in the folder
                    rename_cmd = 'for i in ./%s/*.*; do  ' \
                                 ' ext="${i##*.}"      ; ' \
                                 ' fil="${i%%.*}-c%s"  ; ' \
                                 ' mv "$i" "$fil.$ext" ; ' \
                                 'done' % (dir_local, cn)
                    local(rename_cmd)
                    local('rm -rf %s' % tempdir_local)
                else:
                    tempdir_local = '%s/c%s' % (tempfile.gettempdir(), cn)
                    bz2err_remote = '%s-c%s-err.bz2' % (f0, cn)
                    bz2out_remote = '%s-c%s-out.bz2' % (f0, cn)
                    bz2err_full_local = '%s/%s-err.bz2' % (tempdir_local, f0)
                    bz2out_full_local = '%s/%s-out.bz2' % (tempdir_local, f0)
                    # packing
                    print blue('c%s packing ...' % cn)
                    run('tar -jcvf %s %s.err' % (bz2err_remote, f0))
                    run('tar -jcvf %s %s.out' % (bz2out_remote, f0))
                    # download them
                    print blue('c%s transferring to %s...' % (cn, tempdir_local))
                    get(bz2err_remote, bz2err_full_local)
                    get(bz2out_remote, bz2out_full_local)
                    # the files are here, remove remote bz2
                    run('rm -f %s' % bz2err_remote)
                    run('rm -f %s' % bz2out_remote)
                    # unpacking to temp dir
                    print blue('c%s unpacking ...' % cn)
                    local('tar -xvf %s -C %s' % (bz2err_full_local, tempdir_local))
                    local('tar -xvf %s -C %s' % (bz2out_full_local, tempdir_local))
                    # unpacked ok, remove local bz2
                    #local('rm -f %s' % bz2err_full_local)
                    #local('rm -f %s' % bz2out_full_local)
                    print blue('c%s moving to current dir ...' % cn)
                    local('mv %s/%s.err ./%s-c%s.err' % (tempdir_local, f0, f0, cn))
                    local('mv %s/%s.out ./%s-c%s.out' % (tempdir_local, f0, f0, cn))
                    local('rm -rf %s' % tempdir_local)


@roles('client')
def kill(force=False):
    """Kills YCSB processes"""
    with settings(warn_only=True):
        run('ps -f -C java')
        if force or confirm(red("Do you want to kill Java on the client?")):
            run('killall java')

@roles('client')
def clean_logs(force=False, db=None):
    """Removes all logs from /run/shm"""
    home = '/run/shm'
    if db:
        database = get_db(db)
        home = database['home']
    if force or confirm(red("Do you want to clear all logs from RAM?")):
        run('rm -rf %s/*' % home)

@runs_once
def _build_and_upload():
    local('mvn clean package')
    put('distribution/target/ycsb-0.1.4.tar.gz', '~/ycsb.tar.gz')

@roles('all_client')
def deploy():
    """Builds and deploys YCSB to the clients"""
    _build_and_upload()
    client1 = env.roledefs['client'][0]
    run('scp %s:ycsb.tar.gz .' % client1)
    with cd('/opt'):
        run('ln -sf ycsb-0.1.4 ycsb')
        run('rm -rf ycsb-0.1.4')
        run('tar xzvf ~/ycsb.tar.gz')
        
