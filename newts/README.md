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

## Installing NewTS

NewTS uses Cassandra as Datastore. As such you will need to set up both cassandra and NewTS.
There is an official tutorial to set up NewTS at [their github wiki][gh-wiki-getting-started]

For all purposes we need the following programs:

 - Java 8+
 - curl
 - tar
 - sed

### Steps

 1. Set up cassandra, for example as follows:
     1. Download the latest 3.0.x cassandra version 
     2. start cassandra in the foreground in terminal 1
 2. Download and extract NewTS, for example with the following shell invocations.
  
     ```bash
     curl -L https://github.com/OpenNMS/newts/releases/download1.4.3/newts-1.4.3-bin.tar.gz -o newts-1.4.3-bin.tar.gz
     tar xvf newts-1.4.3-bin.tar.gz
     cd newts-1.4.3
     ```
     
 3. Configure and initialize NewTS.
     Adjust the default configuration at `etc/config.yml` if you did not install cassandra to your localhost with the default settings.
     Then run the one-time initialization procedure using `bin/init etc/config.yml`.
     
 4. Start NewTS in the foreground:

    ```bash
    bin/newts -c etc/config.yml
    ```
 
You should now be able to test your installation by running some basic tests like the following:

```bash
curl https://raw.githubusercontent.com/OpenNMS/newts/master/rest/samples.txt -o samples.txt
curl -D - -X POST -H "Content-Type: application/json" -d @samples.txt http://0.0.0.0:8080/samples
curl -D - -X GET 'http://0.0.0.0:8080/samples/localhost:chassis:temps?start=1998-07-09T12:05:00-0500&end=1998-07-09T13:15:00-0500'; echo
```

There also is a simple web UI that you can use for testing at `http://host:8080/ui/`.

## YCSB Integration

The binding requires the following properties to be set:

 - `ip` **hostname to query**:
   The hostname that NewTS is reachable under.

 - `port` **NewTS exposed port**:
   The port that NewTS is exposing it's REST API on.

Additional tuning options include:

 - `keySpace` **Cassandra session keyspace**:
    The keyspace to use for the cassandra session of NewTS.
    Defaults to `newts`.


 - `filterForTags` **do Tag filtering**:
    NewTS does not fully support tag filtering.
    One can **either** filter for tags or filter for timestamps.
    If this property is set to `false`, the binding will *always* filter for Timestamps,
    otherwise it will do so if the filter criteria do not include tags.
    Defaults to `false`.

 - `debug` **enable debug logging**:
  Verbose debug information is logged by the ycsb-binding.

 - `test` **test run**:
  If set to true, the ycsb-binding will build (and possibly log) queries but not execute them.

## YCSB Runs

The binding supports only timestamp workloads.
If non-timestamp-workloads are used, the binding is highly likely to not work.

```bash
bin/ycsb load newts -P workload/tsworkloada -P newts.properties
bin/ycsb run newts -P workload/tsworkloada -P newts.properties
```


 [gh-wiki-getting-started]: https://github.com/OpenNMS/newts/wiki/GettingStarted