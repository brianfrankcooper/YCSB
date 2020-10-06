<!--
Copyright (c) 2020 YCSB contributors. All rights reserved.

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

# Quick Start

This section describes how to run and configure YCSB on Elasticsearch 7.x. This package uses [Java High Level REST Client][1]: the official high-level client for Elasticsearch.

## 1. Install and Start Elasticsearch

Follow the instructions you can find [here][2] to install and start Elasticsearch 7.x. In this site you can see the installation steps for various operating systems.

## 2. Set up YCSB

Download the [latest release of YCSB][3] and follow the instructions:

```sh
curl -O --location https://github.com/brianfrankcooper/YCSB/releases/download/0.17.0/ycsb-0.17.0.tar.gz
tar xfvz ycsb-0.17.0.tar.gz
cd ycsb-0.17.0
```

## 3. Run YCSB

To know how to run YCSB follow the instructions that you can find [here][4].

### Load data

```sh
-/bin/ycsb load elasticsearch7 -P workloads/workloada > outputLoad.txt
```

### Load workload

```sh
-/bin/ycsb run elasticsearch7 -P workloads/workloada > outputRun.txt
```

### Settings parameters

- `es.cluster.name`: (String) Elasticsearch cluster name . 

> One or more nodes that share the same cluster name. Source: [Elasticsearch glossary][5].

- `es.index.name`:  (String) Elasticsearch index name.

> An optimized collection of JSON documents. Each document is a collection of fields, the key-value pairs that contain your data. Source: [Elasticsearch glossary][5].

- `es.index.key`: (String) The record key, it corresponds to _id field.

> The ID of a document identifies a document. The index/id of a document must be unique. If no ID is provided, then it will be auto-generated. Source: [Elasticsearch glossary][5].

- `es.number_of_shards`: (Integer) Number of shards.

> A shard is a single Lucene instance. It is a low-level “worker” unit which is managed automatically by Elasticsearch. Source: [Elasticsearch glossary][5].

- `es.number_of_replicas`: (Integer) Number of shards.

> Each primary shard can have zero or more replicas. A replica is a copy of the primary shard. Source: [Elasticsearch glossary][5].

- `es.hosts.list`: (List) The hosts on which the Elasticsearch nodes are deployed. Note: Each host has the structure: `host:port`. The hosts are separated by a comma.

> A node is a running instance of Elasticsearch which belongs to a cluster. Source: [Elasticsearch glossary][5].

- `es.security.ssl`: (Boolean) If your cluster traffic is encrypted using SSL/TLS.

> Preserving the integrity of your data with SSL/TLS encryption. See [Encrypting communications][6]

- `es.security.ssl.path`: (String) Path to Elasticsearch certificates. It is recommended to use `.p12` certificates.

> TLS requires X.509 certificates to perform encryption and authentication of the application that is being communicated with. See [Generating node certificates][7]

- `es.authentication`: (Boolean) If your cluster is protected using an authentication process.

> This configuration restricts the access to the cluster offering an authentication process to identify the users behind the requests that hit the cluster. The user must prove their identity, via passwords or credentials. See [User authorization][8]

- `es.credentials.user `: (String) The authenticated user.

> Username. See [User authenticarion][9]

- `es.credentials.password`: (String) The authenticated password.

> Password. See [User authenticarion][9]

### Default settings

If you do not set custom settings to run YCSB on Elasticsearch 7.x the configuration is:

```sh
es.cluster.name=elasticsearch
es.index.name=es.ycsb
es.index.key=key
es.number_of_shards=1
es.number_of_replicas=0
es.hosts.list=0.0.0.0:9200
es.security.ssl=false
es.security.ssl.path=/root/certificates.p12
es.authentication=false
es.credentials.user=elastic
es.credentials.password=changeme
```

### Custom settings

If you want a custom configuration for running YCSB in Elasticsearch 7.x, you can create a configuration file. This file should have the extension `.data` and is passed to YCSB as follows:

```sh
./bin/ycsb run elasticsearch7 -P workloads/workloada -P myproperties.data -s
```

For parameters not customized by the user, the default values will be used.

**CONFIGURING SECURITY PARAMETERS**

Elasticsearch 7.x Java High Level REST Client for YCSB framework enables the user to connect to clusters that have different levels of security provided by [Elasticsearch security features][10]. 

| Security Level | `es.security.ssl` | `es.authentication` |
| ------ | ------ | ------ |
| SSL certificates and authentication | true | true |
| Authentication | false | true |
| SSL certificates | true | false |
| Nothing | false | false |

[1]: https://www.elastic.co/guide/en/elasticsearch/client/java-rest/master/java-rest-high.html 
[2]: https://www.elastic.co/guide/en/elasticsearch/reference/7.x/install-elasticsearch.html
[3]: https://github.com/brianfrankcooper/YCSB/releases/latest
[4]: https://github.com/brianfrankcooper/YCSB/wiki/Core-Workloads
[5]: https://www.elastic.co/guide/en/elasticsearch/reference/current/glossary.html#index
[6]: https://www.elastic.co/guide/en/elasticsearch/reference/current/encrypting-communications.html
[7]: https://www.elastic.co/guide/en/elasticsearch/reference/7.9/configuring-tls.html#node-certificates
[8]: https://www.elastic.co/guide/en/elasticsearch/reference/current/authorization.html
[9]: https://www.elastic.co/guide/en/elasticsearch/reference/current/setting-up-authentication.html
[10]: https://www.elastic.co/guide/en/elasticsearch/reference/current/configuring-security.html