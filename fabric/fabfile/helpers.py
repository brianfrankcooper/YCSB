from datetime import datetime, timedelta
import os
import re

from fabric.context_managers import settings, hide
from fabric.operations import run
from conf import hosts, databases, workloads

basetime = None


def base_time(time=None, round_sec=120, tz = hosts.timezone):
    """
    Get the next timestamp rounded to round_sec seconds
    the function returns some future time rounded accordingly
    to round_sec parameter.
    Examples: 2:22:29 -> 2:23:00
              2:22:31 -> 2:24:00
    """
    global basetime
    if basetime is None: basetime = datetime.now(tz)
    if time is None: time = basetime
    begin = time.min.replace(tzinfo=tz) # long time ago
    seconds = (time - begin).seconds
    rounding = (seconds + round_sec * 1.5) // round_sec * round_sec
    return time + timedelta(0, rounding-seconds, -time.microsecond)# + timedelta(60*24) #To the next day issue

def almost_nothing():
    #return settings(hide('running', 'warnings', 'stdout', 'stderr'), warn_only=True)
    return settings(hide(), warn_only=True)
   

def get_db(database):
    if not databases.databases.has_key(database):
        raise Exception("unconfigured database '%s'" % database)
    return databases.databases[database]

def get_workload(workload):
    if not workloads.workloads.has_key(workload):
        raise Exception("unconfigured workload '%s'" % workload)
    return workloads.workloads[workload]

def get_outfilename(databasename, workloadname, extension, the_time, target=None):
    the_time_str = the_time.strftime('%Y-%m-%d_%H-%M')
    if target is None:
        return '%s_%s_%s.%s' % (the_time_str, databasename, workloadname, extension)
    else:
        return '%s_%s_%s_%s.%s' % (the_time_str, databasename, workloadname, str(target), extension)

def get_properties(database, workload=None):
    properties = {}
    for (key, value) in workloads.data.items():
        properties[key] = value
    for (key, value) in database['properties'].items():
        properties[key] = value
    if workload and workload.has_key('properties'):
        for (key, value) in workload['properties'].items():
            properties[key] = value
    return properties

def _sh_quote(argument):
    return '"%s"' % (
        argument
        .replace('\\', '\\\\')
        .replace('"', '\\"')
        .replace('$', '\$')
        .replace('`', '\`')
        )

def _at(cmd, time=base_time()):
    return 'echo %s | at %s today' % (_sh_quote(cmd), time.strftime('%H:%M'))


def determine_file(regex):
    is_dir = False
    p = re.compile(regex)
    ls = run('ls --format=single-column --sort=t *.err *.out').split("\r\n")
    file_names = [f for f in ls if p.search(f)]
    if len(file_names) > 0:
        # the most recent file satisfying pattern
        (f0, f1) = os.path.splitext(file_names[0]) # split to (path, ext)
        return f0, is_dir
    else:
        # list dirs only
        ls = run('ls --format=single-column --sort=t -d -- */').split("\r\n")
        dir_names = [f.strip('/') for f in ls if p.search(f)]
        f0 = dir_names[0]
        is_dir = True
        return f0, is_dir