# SickStore Driver for YCSB
This driver is a binding for the YCSB facilities to operate against a SickStore Server.

## Quickstart

### 1. Set up YCSB (Fork from steffenfriedrich) and SickStore
```
git clone git://github.com/steffenfriedrich/YCSB.git
cd YCSB
```

### 2. Start SickStore Server
...


## Configuration Options

### YCSB-binding
 - sickstore.url=localhost => The connection URL.
 - sickstore.port=54000 => 
 - sickstore.timeout=1000 => ...
 - sickstore.write_concern.ack=1 => The number of acknowledgments from replicas (or a tag set)
 - sickstore.write_concern.journaling=false => Simulate a journal commit?
