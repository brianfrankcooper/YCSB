<!--
Copyright (c) 2018 YCSB contributors. All rights reserved.

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

## Installing hdf5 and h5serv

h5serv directly includes the implementation for hdf5, as such it's only necessary to install h5serv.
That can be accomplished by following the [h5serv installing tutorial][h5serv-tutorial].

h5serv uses Python 2.7 or later. You will need a python installation to run it.
It's verified to work correctly with the python versions: `2.7`, `3.4`, `3.5` and `3.6`.

In essence the following steps are necessary:

- Install the required python packages, namely
    - NumPy 1.10.4 or later
    - h5py 2.5 or later
    - tornado 4.0.2 or later
    - watchdog 0.8.3 or later (not available through anaconda)
    - requests 2.3 or later (for client tests)
 The packages can be installed manually or through anaconda.
- Download hdf5-json from `github.com/HDFGroup/hdf5-json` (either through git clone or as zip)
- Install hdf5-json by running `python setup.py install` in the hdf5-json sources folder
- Download h5serv sources (either through git clone or as zip)
- The server configuration is under `{sources directory}/server/config.py`.
 You may adjust it according to your preferences.
- Finally the server can be started by running `python app.py` in the `server` directory of h5serv.

Lastly one can optionally verify the installation by running the following commands:
```
source activate h5serv # omit "source" on Windows
cd {sources directory}/test
python testall.py
```

[h5sserv-tutorial]: https://github.com/HDFGroup/h5serv/blob/develop/docs/Installation/ServerSetup.rst

## YCSB Integration

The binding requires the following properties to be set:

- `ip` **hostname or ip**:
 The IP or Hostname that your hdf5 installation is available under.

- `port` **HTTP API port**:
 The port that hdf5 exposes it's REST Api under.
 Defaults to 5000.

Additional options include:

- `debug` **enable debug logging**:
 Verbose debug information is logged by the ycsb-binding.

- `test` **test run**:
 If set to true, the ycsb-binding will build (and possibly log) queries but not execute them.

## YCSB Runs

The binding supports only timestamp workloads.
If non-timestamp-workloads are used, the binding is highly likely to not work.

```bash
bin/ycsb load h5serv -P workload/tsworkloada -P h5serv.properties
bin/ycsb run h5serv -P workload/tsworkloada -P h5serv.properties
```
