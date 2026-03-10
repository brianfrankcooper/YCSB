<!--
Copyright (c) 2026 YCSB contributors. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License"); you
may not use this file except in compliance with the License. You
may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License. See accompanying
LICENSE file.
-->

## Quick Start

This binding benchmarks `kvraft` through its gRPC `KVService` API.

### 1. Build the binding

```bash
git clone https://github.com/brianfrankcooper/YCSB.git
cd YCSB
mvn -pl site.ycsb:kvraft-binding -am clean package
```

### 2. Start a kvraft cluster

Point YCSB at any reachable kvraft client address. The binding follows leader
hints returned in `KVResponse.leader`.

### 3. Configure the binding

Required property:
- `kvraft.target`

Optional properties:
- `kvraft.tableprefix`
  - Prefix applied before YCSB keys.
  - Default: `<table>:`
- `kvraft.rpc_timeout_ms`
  - Default: `3000`
- `kvraft.max_redirects`
  - Number of leader-hint redirects to follow before failing.
  - Default: `3`

### 4. Load and run

```bash
./bin/ycsb.sh load kvraft -P workloads/workloadb -p kvraft.target=127.0.0.1:8000 -p fieldcount=1 -s
./bin/ycsb.sh run kvraft -P workloads/workloadb -p kvraft.target=127.0.0.1:8000 -p fieldcount=1 -s
```

## Semantics

- `read` maps to `Get`
- `insert` maps to `Put`
- `update` reads the current document, merges updated fields, then writes the full record back with `Put`
- `delete` maps to `Delete`
- `scan` is not implemented

This binding stores each YCSB record as a JSON document under one kvraft key.

For the cleanest benchmark contract, use `fieldcount=1`. Direct leader targeting
removes one redirect hop, but the binding can recover automatically if you point
it at a follower.
