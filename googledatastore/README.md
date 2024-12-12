<!--
Copyright (c) 2024 YCSB contributors.
All rights reserved.

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

# Google Cloud Datastore Binding

https://cloud.google.com/datastore/docs/concepts/overview?hl=en

Please refer [here] (https://cloud.google.com/datastore/docs/apis/overview) for more information on
Google Cloud Datastore API.

## Configure

    YCSB_HOME - YCSB home directory
    DATASTORE_HOME - Google Cloud Datastore YCSB client package files

Please refer to https://github.com/brianfrankcooper/YCSB/wiki/Using-the-Database-Libraries
for more information on setup.

# Benchmark

    $YCSB_HOME/bin/ycsb load googledatastore -P workloads/workloada -P googledatastore.properties
    $YCSB_HOME/bin/ycsb run googledatastore -P workloads/workloada -P googledatastore.properties

# Properties

    $DATASTORE_HOME/conf/googledatastore.properties

# Details

## Configuration and setup:

See this link for instructions about setting up Google Cloud Datastore and
authentication:

https://cloud.google.com/datastore/docs/activate#accessing_the_datastore_api_from_another_platform

After you setup your environment, you will have 4 pieces of information ready:
- projectId,
- datasetId (omit to use the default database),
- service account email, and
- a private key file in P12 format.

These will be configured via corresponding properties in the googledatastore.properties file.

## EntityGroupingMode

In Google Datastore, Entity Group is the unit in which the user can
perform strongly consistent query on multiple items; Meanwhile, Entity group
also has certain limitations in performance, especially with write QPS.

We support two modes here:

1. [default] One entity per group (ONE_ENTITY_PER_GROUP)

In this mode, every entity is a "root" entity and sits in one group,
and every entity group has only one entity. Write QPS is high in this
mode (and there is no documented limitation on this). But query across
multiple entities are eventually consistent.

When this mode is set, every entity is created with no ancestor key (meaning
the entity itself is the "root" entity).

2. Multiple entities per group (MULTI_ENTITY_PER_GROUP)

In this mode, all entities in one benchmark run are placed under one
ancestor (root) node therefore inside one entity group. Query/scan
performed on these entities will be strongly consistent but write QPS
will be subject to documented limitation (current is at 1 QPS).

Because of the write QPS limit, it's highly recommended that you rate
limit your benchmark's test rate to avoid excessive errors.

The goal of this MULTI_ENTITY_PER_GROUP mode is to allow user to
benchmark and understand performance characteristics of a single entity
group of the Google Datastore.

While in this mode, one can optionally specify a root key name. If not
specified, a default name will be used.

## Enable tracing for benchmarking
 To enable publishing traces to Google Cloud Trace while running benchmarking tests, `googledatastore.tracingenabled` must be set.
 Tracing depends on the following external APIs/Services:
1. Java OpenTelemetry SDK
2. Cloud Trace Exporter
3. TraceServiceClient from Cloud Trace API v1.

You wil need to have these permissions to enabled tracing feature:
1. gcloud auth application-default login must be run with the test user.
2. To write traces, test user must have one of roles/cloudtrace.[admin|agent|user] roles.
3. To read traces, test user must have one of roles/cloudtrace.[admin|user] roles.

For more details about setting permissions, please refer to [Permissions and predefined Cloud Trace roles
](https://cloud.google.com/trace/docs/iam#trace-roles).


