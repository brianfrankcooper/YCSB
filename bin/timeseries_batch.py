from __future__ import print_function
import csv
import os
import sys
import timeseries_draw
import timeseries_merge
from guppy import hpy

def update_collect(collect):
    draw_name = ''
    draw_stats = {}
    with open(sys.argv[1]) as fin:
        block = 0
        reader = csv.reader(fin, dialect='excel-tab')
        for items in reader:
            if len(items) == 0:
                block += 1
            else:
                if block == 0:
                    draw_name = items[0]
                elif block == 1:
                    pass
                elif block == 2:
                    pass
                elif block == 3:
                    pass
                else:
                    # try to convert to float, and if not successs, then leave as is
                    try:
                        draw_stats[items[0]] = float(items[1])
                    except ValueError:
                        draw_stats[items[0]] = items[1]
        if len(draw_stats) > 0:
            collect.append((draw_name, draw_stats))
            # this script goes over all dirs and subdirs under

keyz_ram_str = """
aerospike26newclientspersistenceondevice-sync-failover_ram-workloada_12500-50_percent_max_throughput
aerospike26newclientspersistenceondevice-sync-failover_ram-workloada_18750-75_percent_max_throughput
aerospike26newclientspersistenceondevice-sync-failover_ram-workloada-no_limit_percent_max_throughput
aerospike26newclientspersistenceondevice-async-failover_ram-workloada_25000-50_percent_max_throughput
aerospike26withwriteduplicatedisabletrue-async-failover_ram-workloada_25000-50_percent_max_throughput
aerospike26withwriteduplicatedisabletrue-async-failover_ram-workloada_37500-75_percent_max_throughput
aerospike26withwriteduplicatedisabletrue-async-failover_ram-workloada-no_limit_percent_max_throughput
cassandra-async-failover_ram-workloada_2500-50_percent_max_throughput
cassandra-async-failover_ram-workloada_3750-75_percent_max_throughput
cassandra-async-failover_ram-workloada-not_limit_percent_max_throughput
cassandra-sync-failover_ram-workloada_1875-50_percent_max_throughput
cassandra-sync-failover_ram-workloada_2750-75_percent_max_throughput
cassandra-sync-failover_ram-workloada-not_limit_percent_max_throughput
couchbase2-async-failover_ram-workloada_31250-50_percent_max_throughput
couchbase2-async-failover_ram-workloada_46875-75_percent_max_throughput
couchbase2-async-failover_ram-workloada-no_limit_max_throughput
mongo-async-failover_ram-workloada_2812-50_percent_max_throughput
mongo-async-failover_ram-workloada_4218-75_percent_max_throughput
mongo-async-failover_ram-workloada-not_limit_percent_max_throughput
aerospike-async-failover_ram-workloada_25000-50_percent_max_throughput
aerospike-async-failover_ram-workloada_37500-75_percent_max_throughput
aerospike-async-failover_ram-workloada-not_limit_percent_max_throughput
aerospike-sync-failover_ram-workloada_12500-50_percent_max_throughput_3
aerospike-sync-failover_ram-workloada_18750-75_percent_max_throughput
aerospike-sync-failover_ram-workloada-not_limit_percent_max_throughput
aerospike-sync-failover_ram-workloada_12500-50_percent_max_throughput_workloada_with_write-duplicate-resolution-disablet
aerospike26-async-failover_ram-workloada_12500-50_percent_max_throughput
aerospike26-sync-failover_ram-workloada_12500-50_percent_max_throughput
aerospike26newclients-async-failover_ram-workloada_12500-50_percent_max_throughput
aerospike26newclients-sync-failover_ram-workloada_12500-50_percent_max_throughput
aerospike26newclientsuniform-async-failover_ram-workloada_12500-50_percent_max_throughput
aerospike26newclientsuniform-sync-failover_ram-workloada_12500-50_percent_max_throughput
aerospike26newclientswithoutretry-async-failover_ram-workloada_12500-50_percent_max_throughput
aerospike26newclientswithoutretry-sync-failover_ram-workloada_12500-50_percent_max_throughput
"""

if __name__ == "__main__":

    # prefix and selects all paths that do 'failover_ram'
    # tests. For each path under the condition the graph is built
    # prefix = "/home/nick/buffer/Aerospike/Aerospike26NewClients"
    prefix = "/home/nick/buffer/Aerospike/"

    # postfix - where the new graphs will be put
    postfix = "/home/nick/buffer/Aerospike/XGraphs/"
    # if file paths.txt exists, then load paths from it
    # otherwise walk over prefix dir

    paths = []
    for path in os.walk(prefix):
        # if this path is for failover_ram and has no chils, e.g.
        # '/home/nick/buffer/Aerospike/Aerospike/failover_ram/async/50_percent_max_throughput'
        if 'failover' in path[0] and len(path[1]) == 0:
            paths.append(path[0])
    # paths = filter(lambda s: "Aerospike26" in s, paths)
    # now the list of dirs formed, generate the pictures
    h = hpy()
    collect = []
    for path in paths:
        if path == '/home/nick/buffer/Aerospike/Aerospike26NewClients/failover_ram/sync/50_percent_max_throughput':
            print('here')
        os.chdir(path)
        sys.argv = ["", "series.txt"]
        if True:
            try:
                timeseries_merge.merge(collect)
                name = timeseries_draw.draw()
                # move this new wonderful file to XGraphs
                src_name = timeseries_draw.file_name_with_ext(name)
                tgt_name = postfix + src_name
                os.rename(src_name, tgt_name)
            except:
                print(sys.exc_info()[0], file=sys.stderr)
        else:
            update_collect(collect)
        print("done with %s" % path)
        # print("memory = %s" % h.heap())

    keyz_ram = filter(lambda s: len(s) > 0, keyz_ram_str.split("\n"))
    with open("/home/nick/buffer/collect.txt", "w") as f:
        cw = csv.writer(f, dialect='excel-tab')
        collect.sort(key=lambda c: c[0])
        # stable sort, we group first 'failover_ram', using False < True
        def percents(nm):
            if '50_percent' in nm: return 50
            elif '75_percent' in nm: return 75
            elif 'limit_percent' in nm: return 100
            else: return 0
        collect.sort(key=lambda c: ('failover_ram' not in c[0], percents(c[0])))
        # stable sort, no_limit > 50% and 75%
        def key_index(nm):
            if nm in keyz_ram:
                return keyz_ram.index(nm)
            else:
                return 999
        collect.sort(key=lambda c: key_index(c[0]))
        # sort complete
        for c in collect:
            lt_nd = c[1]['_lt_nd']
            lt_nu = c[1]['_lt_nu']
            row = [c[0], lt_nd, lt_nu]
            cw.writerow(row)

    # ts_merge = "/home/nick/ycsb/bin/timeseries_merge.py"
    # ts_draw  = "/home/nick/ycsb/bin/timeseries_draw.py"
    # os.system("cd %s; %s | %s" % (path, ts_merge, ts_draw))
    print("all walking done!")
