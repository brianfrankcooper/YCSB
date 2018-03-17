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

## Installing KDB+

The full installation instructions can be found at the [official documentation][official-docs].

The instructions are somewhat different, depending on the operating system you're installing on.
This guide will only contain the installation instructions for Linux.

### Steps:

 1. Download the official release distribution.
 2. Unzip the release distribution to a folder of your choice.
 3. Determine your operating system bitness.
 4. If you are installing 32-bit kdb+ on an 64-bit OS, you will need a 32-bit version of `libc6`,
    that is either `libc6-i386` or `lic6-i686`.
 5. Verify you can launch kdb+ by calling the  `l32/q` executable from the extracted files.
 
You can subsequently install `rlwrap` to get callbacks and edits to previous commands.  
Furthermore it is recommended to define `q` as a command using the following alias:

```bash
alias q='QHOME=<install folder>/q rlwrap -r <install folder>/q/l32/q'
```

Lastly you can verify the installation by calling `til 6` in the `q` command prompt.
The expected output is:

```text
0 1 2 3 4 5
```

## YCSB Integration 

The binding requires the following properties to be set:

 - `ip` **kdb+ installation host**: 
  Should usually default to `localhost`.

 - `port` **kdb+ port reachability**:
  Should usually default to `5001`.

Additional tuning options include:

 - `debug` **enable debug logging**:
  Verbose debug information is logged by the ycsb-binding.

 - `test` **test run**:
  If set to true, the ycsb-binding will build (and possibly log) queries but not execute them.
  
## YCSB Runs
  
The binding supports only timestamp workloads.
If non-timestamp-workloads are used, the binding is highly likely to not work.
  
```bash
bin/ycsb load kdbplus -P workload/tsworkloada -P kdbplus.properties
bin/ycsb run kdbplus -P workload/tsworkloada -P kdbplus.properties
```


 [official-docs]: http://code.kx.com/q/tutorials/install/