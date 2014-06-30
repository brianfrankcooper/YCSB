#!/usr/bin/python

from __future__ import print_function
import os
import re
import string
import sys
import ConfigParser as configparser
from functools import partial
from itertools import chain
# import pkg_resources
# print(pkg_resources.get_distribution("matplotlib").version)

# helper class to avoid MissingSectionHeaderError in ConfigParser
# takes a stream, and inserts empty section before
class Helper:
    def __init__(self, section, file):
        self.readline = partial(next, chain(("[{0}]\n".format(section),), file, ("",)))

def avg(seq):
    return sum(seq) / float(len(seq))

def same(x): return x

def scale1k(x) : return x / 1000.0

def merge(collect = None):
    """grab all *.out, extract statistics from there and merge into TSV file """
    throughput = {}
    update_latency = {}
    read_latency = {}
    items = filter(lambda x: str(x).endswith('.out'), os.listdir('.'))
    # common_name is the name of items without '-ci' part, e.g.
    # 2013-02-07_21-40_couchbase_workloada_31250
    # then we try to cut off some parts
    common_name = os.path.commonprefix(items)[:-2]
    common_db_name = 'unknown'
    # remove date-time and database name, since it is already there
    m = re.search(r'\d{4}-\d{2}-\d{2}_\d+-\d+_([^_]+)_(.+)', common_name)
    if m is not None:
        common_name = m.group(2)
        common_db_name = m.group(1)
    the_plist = split_path(os.getcwd())
    the_plist.reverse()
    the_plist = map(lambda s: s.lower(), the_plist)
    # db_name try to find in dir hierarchy, if not success, then fallback
    db_name = find_set(["aerospike", "cassandra", "couchbase2", "mongo"], the_plist, 5, common_db_name)
    repl_type = find_set(["sync", "async"], the_plist, 4, "async")
    # find in the_plist first element that contains "failover" word,
    # usually it is "failover_ram" or "failover_ssd"
    def contains_failover(s):
        return 'failover' in s
    ram_or_ssd = next((x for x in the_plist if contains_failover(x)), "failover")
    # now combine the name of the graph
    # graph_name ~ 'aerospike-async-failover_ram-workloada_25000'
    graph_name = '-'.join([db_name, repl_type, ram_or_ssd, common_name, the_plist[0]])
    # sanitize file name, it should not contain bad characters and be not too long
    valid_chars = "-_.() %s%s" % (string.ascii_letters, string.digits)
    graph_name = ''.join(c for c in graph_name if c in valid_chars)[0:120]
    # try to load and parse stats.conf
    # this file contains additional data to draw
    # stats is a dictionary containing values
    config_parser = configparser.RawConfigParser()
    try:
        with open("stats.conf") as sf:
            config_parser.readfp(Helper("Dummy", sf))
            stats = map_config(config_parser, "Dummy")
    except: # IOError
        stats = {}
    
    # gather stats from all files=items
    for item in items:
        correction_time = 0
        #correction_time is correction about ycsb client's delay
        #c[1-8].correction.cfg contains positive or negative int correction time in ms
        #with open("%s.correction.cfg" % item[-6:-4]) as cf:
        #    correction_time = int(cf.read())


        with open(item) as f:
            for line in f:
                if line.startswith('[UPDATE]') or line.startswith('[READ]'):
                    items = line.split(',')
                    if len(items) == 4:
                        (op, timestamp, lat, thr) = items
                        timestamp = int(timestamp)
                        lat = float(lat) / 1000.0
                        thr = thr.strip().split(' ', 1)[0]
                        try:
                            thr = float(thr)
                        except ValueError:
                            #For "[UPDATE], 1575400, 16432.262857142858, 5250.0Reconnecting to the DB..." line
                            thr = float(re.search('\d+\.\d+', thr).group(0))
                        
                        timestamp = timestamp - correction_time
                        #To avoid negative time
                        if timestamp < 0:
                            timestamp = 0

                        thr_stats = throughput.get(timestamp, 0)
                        thr_stats += thr
                        
                        #To avoid cumulative throughput on 0 time point 
                        if timestamp == 0:
                            thr_stats = 0
                             
                        throughput[timestamp] = thr_stats
                        # default latensy 0 will not work properly
                        # hence this statement is totally wrong:
                        if (op == '[READ]'):
                            stats1 = read_latency.get(timestamp, [])
                            stats1.append(lat)
                            read_latency[timestamp] = stats1
                        elif (op == '[UPDATE]'):
                            stats2 = update_latency.get(timestamp, [])
                            stats2.append(lat)
                            update_latency[timestamp] = stats2
    # Let's count practically zero throughput time after node down
    # but before and after node up. We count very low values as being zero
    t_node_down = 600000
    t_node_up  = 1200000
    if len(throughput) > 0:
        bound = max(throughput.values()) * 0.1
        lt_before_node_up = 0
        lt_after_node_up = 0
        for t in range(0, 2400000, 100):
            v = throughput.get(t)
            if v is None or v < bound:
                if t_node_down <= t < t_node_up:
                    lt_before_node_up += 100
                elif t_node_up <= t:
                    lt_after_node_up += 100
        # _lt_nd (and _lt_nu) are abbreviations of
        # Low Throughput wen Node Down (and Up)
        stats['_lt_nd'] = lt_before_node_up
        stats['_lt_nu'] = lt_after_node_up
        stats['_name'] = graph_name
        if collect is not None:
            collect.append((graph_name, stats))
    #for (timestamp, thr) in OrderedDict(sorted(throughput.items(), key=lambda t: t[0])).items():
    # convert throughput dict to sorted list
    thr_list = throughput.items()
    thr_list.sort(key=lambda (x, y): x)
    if len(sys.argv) > 1:
        # filename passed, to filename
        with open(sys.argv[1], 'w') as f:
            flush_series(f, graph_name, stats, read_latency, update_latency, thr_list)
    else:
        # to stdout
        flush_series(sys.stdout, graph_name, stats, read_latency, update_latency, thr_list)

def split_path(path, maxdepth=20):
    ( head, tail ) = os.path.split(path)
    return split_path(head, maxdepth - 1) + [ tail ] \
        if maxdepth and head and head != path \
        else [ head or tail ]

def find_set(words, the_list, max_depth, default = ""):
    words_set = set(words)
    local_list = the_list[0:max_depth]
    for word in local_list:
        if word in words_set:
            return word
        else:
            for w in words_set:
                if word.startswith(w):
                    return word
    return default

def map_config(configparser, section):
    dict1 = {}
    options = configparser.options(section)
    for option in options:
        try:
            dict1[option] = configparser.get(section, option)
            if dict1[option] == -1:
                print("skip: %s" % option, f=sys.stderr)
        except:
            print("exception on %s!" % option)
            dict1[option] = None
    return dict1

def avg_convert(the_dict):
    # post-processing for latencies goes here
    list1 = the_dict.items()
    list2 = [(x, avg(y)) for (x, y) in list1 if len(y) > 0]
    # remove empty elements
    list2.sort(key=lambda (x, y): x)
    # convert each (sub)list to average
    return list2


def flush_series(f, graph_name, stats, read_latency, update_latency, thr_list):
    # # write graph name as a first piece of data
    # print(graph_name, file=f)
    # additional stats
    # print('', file=f)
    for pair in stats.items():
        print(tab_str(pair), file=f)
    # block with read latency
    print('', file=f)
    for t in avg_convert(read_latency):
        print(tab_str(t), file=f)
    # block with update latency
    print('', file=f)
    for t in avg_convert(update_latency):
        print(tab_str(t), file=f)
    # block with throughput
    print('', file=f)
    for t in thr_list:
        print(tab_str(t), file=f)

def tab_str(seq):
    return '\t'.join(map(str, seq))

if __name__=='__main__':
    merge()
