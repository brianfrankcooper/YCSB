<!--
Copyright (c) 2017 YCSB contributors. All rights reserved.

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
## Installing Akumuli

Follow the installation instructions from the [Akumuli Github Repository][akumuli-installation].
Akumuli can be installed on Ubuntu using the repository [`packagecloud.io/Lazin/Akumuli`][package-repo] or using [these docker instructions][docker-instructions].

Alternatively it can be built from source.

Akumuli stores configuration in the user-folder after running `akumulid --init`.
A database can be created using `akumulid --create`

Akumuli is then started as a server by running `akumulid` without arguments.
Up-to-date and more in-depth documentation can be found at the [Akumuli Github Repository][akumuli-docs]

[akumuli-installation]: https://github.com/akumuli/Akumuli/wiki/Getting-started
[akumuli-docs]: https://github.com/akumuli/Akumuli/wiki/
[package-repo]: https://packagecloud.io/Lazin/Akumuli
[docker-instructions]: https://hub.docker.com/r/akumuli/akumuli/

## YCSB Integration

You must define the following properties for the ycsb-binding to work:

- `httpPort`:
 The port that your akumuli instance exposes it's HTTP-Endpoint on.
 In a default akumuli configuration this should be 8181.
 Must match the akumuli configuration `HTTP.port`

- `tcpPort`:
 The port that your akumuli instance exposes it's TCP-Endpoint on.
 In a default akumuli configuration this should be 8282.
 Must match the akumuli configuration `TCP.port`

- `ip`:
 The hostname / IP of the remote server that your influxdb instance is running on.
 Must be a valid http hostname for an `http://ip:port/` specification.

Additional options include:

- `debug` **enable debug logging**:
 If set to true, the client will set the highest log-level in the java language bindign for influxdb.
 Additionally verbose debug information is logged by the ycsb-binding.

- `test` **test run**:
 If set to true, the ycsb-binding will build (and possibly log) queries but not execute them.

 ## YCSB Runs

The binding supports only timestamp workloads.
If non-timestamp-workloads are used, the binding is highly likely to not work.

```bash
bin/ycsb load akumuli -P workload/tsworkloada -P influxdb.properties
bin/ycsb run akumuli -P workload/tsworkloada -P influxdb.properties
```
