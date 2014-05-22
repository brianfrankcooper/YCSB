#!/usr/bin/python

import csv

class Interval:

    def __init__(self, begin, end, threshold = 0):
        self._begin = begin
        self._end = end
        self._threshold = threshold
        self._sum = 0
        self._count = 0
        self._completed = False

    def add(self, time, value):
        self._check_completed(time)
        if self._in(time) and self._above(value):
            self._sum += value
            self._count += 1

    def _in(self, time):
        return time >= self._begin and time <= self._end

    def _above(self, value):
        return value > self._threshold

    def _check_completed(self, time):
        if time > self._end:
            self._completed = True

    def average(self):
        if self._count == 0:
            return 0
        return self._sum / self._count

    def completed(self):
        return self._completed

    def threshold(self, threshold=None):
        if threshold:
            self._threshold = threshold
        return self._threshold

    def empty(self):
        return self._count == 0

    def begin(self):
        return self._begin

    def end(self):
        return self._end

def threshold(average):
    """Returns threshold from the initial average latency"""
    return average * 10.0


join_time = 1200000

try:
    with open('stats.conf') as csvfile:
        stats_reader = csv.reader(csvfile, delimiter='=')
        for row in stats_reader:
            if row[0] == 'join to cluster':
                join_time = int(row[1])
except:
    pass

print "join time: %i" % (join_time)

intervals = [
    [
        Interval(1000, 500000),     #initial read
        Interval(500000, 700000),   #read on node down
        Interval(1100000, 1300000),   #read on node up
        Interval(join_time - 100000, join_time + 100000),   #read on node join
    ],
    [
        Interval(1000, 500000),     #initial write
        Interval(500000, 700000),   #write on node down
        Interval(1100000, 1300000),   #write on node up
        Interval(join_time - 100000, join_time + 100000),   #write on node join
    ]
]

data_set_index = -1

with open('series.txt') as csvfile:
    series_reader = csv.reader(csvfile, delimiter="\t")
    for values in series_reader:
        if len(values) == 2:
            if not values[0].isdigit():
                continue
            time = int(values[0])
            if time == 0:
                data_set_index += 1
            if data_set_index > 1:
                break
            value = float(values[1])
            current_intervals = intervals[data_set_index]
            for interval in current_intervals:
                interval.add(time, value)
            if current_intervals[1].threshold() == 0 and current_intervals[0].completed():
                for interval in current_intervals[1:]:
                    interval.threshold(threshold(current_intervals[0].average()))

for i in xrange(0, 4):
    read_interval = intervals[0][i]
    write_interval = intervals[1][i]
    print
    print "between %i and %i" % (read_interval.begin(), read_interval.end())
    print "\tread\twrite"
    print "threshold\t%f\t%f" % (read_interval.threshold(), write_interval.threshold())
    print "latency\t%f\t%f" % (read_interval.average(), write_interval.average())

