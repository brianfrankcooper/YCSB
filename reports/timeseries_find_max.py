#!/usr/bin/python

import sys

begin = int(sys.argv[1])
end = int(sys.argv[2])
print "between %i and %i" % (begin, end)

results = []
index = 0
results.append(0)

for line in sys.stdin:
    values = line.split()
    if len(values) == 2:
        if not values[0].isdigit():
            continue
        time = int(values[0])
        if time >= begin and time <= end:
            value = float(values[1])
            if value > results[index]:
                results[index] = value
        if time > end and results[index] != 0:
            #print time
            index = index + 1
            results.append(0)

print "\t".join(map(str, results[:-1]))
