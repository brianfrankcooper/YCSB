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

# Alibaba Cloud TableStore Driver for YCSB

This driver provides a YCSB workload for Alibaba's hosted TableStore, a fully managed NoSQL cloud database service that enables storage of a massive amount of structured and semi-structured data.

This binding is based on the Java SDK for TableStore.

## Quick Start

### Setup a TableStore Instance

Login to the Alibaba Cloud Console and follow the [Create Instance](https://www.alibabacloud.com/help/doc-detail/55211.htm?spm=a2c63.p38356.b99.17.6822642crAxqTI).
Make a note of the instance name.

### Create a Table for YCSB Testing

Follow the [Create Table](https://www.alibabacloud.com/help/doc-detail/55212.htm?spm=a2c63.p38356.b99.18.1e4e50b9dXCcmC).

The primary key must be exactly one column; the type is 'String'.
Make a note of the table name and primary key name.

### `tablestore.properties`

tablestore.properties is the file specifying information needed by TableStore binding.
The minimal tablestore.properties should be look like:

```
alibaba.cloud.tablestore.access_id = $access_id
alibaba.cloud.tablestore.access_key = $access_key
# the Instance Access URL in 'instance details'
alibaba.cloud.tablestore.end_point = $end_point
alibaba.cloud.tablestore.instance_name = $instance_name
alibaba.cloud.tablestore.primary_key = $primary_key
```

For access_id and access_key, please refer to [Java SDK: Configure an AccessKey](https://www.alibabacloud.com/help/doc-detail/43009.htm?spm=a2c63.p38356.b99.134.728966fcMpTYD1).

### Load and Run a Workload

```zsh
table_name='table_name'

bin/ycsb load tablestore -P workloads/workloada -P tablestore/conf/tablestore.properties -p table=$table_name -threads 2
bin/ycsb run tablestore -P workloads/workloada -P tablestore/conf/tablestore.properties -p table=$table_name -threads 2 
```
