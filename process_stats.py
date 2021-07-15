#!/usr/bin/env python3

import sys
import json
import pprint
from prettytable import PrettyTable
import pdb

file_name = sys.argv[1]

with open(file_name) as f:
    data = json.load(f)

metrics = {}

for point in data:
    metrics[point['metric'] + "-" + point['measurement']] = point['value']

KEY_READ_AVG = 'READ-AverageLatency(us)'
KEY_READ_50PCT = 'READ-50thPercentileLatency(us)'
KEY_READ_99PCT = 'READ-99thPercentileLatency(us)'

METRICS_TO_REPORT = ['READ-AverageLatency(us)',
                     'READ-50thPercentileLatency(us)',
                     'READ-99thPercentileLatency(us)',
                     'READ-MaxLatency(us)',
                     'UPDATE-AverageLatency(us)',
                     'UPDATE-50thPercentileLatency(us)',
                     'UPDATE-99thPercentileLatency(us)',
                     'UPDATE-MaxLatency(us)']

table = PrettyTable()
table.field_names = ["Metric", "Value"]

for key, value in metrics.items():
    if key in METRICS_TO_REPORT:
        table.add_row([key, value])

print(table.get_string())