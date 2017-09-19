<!--
Copyright (c) 2017 YCSB contributors.
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
## How To Contribute

As more and more databases are created to handle distributed or "cloud" workloads, YCSB needs contributors to write clients to test them. And of course we always need bug fixes, updates for existing databases and new features to keep YCSB going. Here are some guidelines to follow when digging into the code.

## Project Source

YCSB is located in a Git repository hosted on GitHub at [https://github.com/brianfrankcooper/YCSB](https://github.com/brianfrankcooper/YCSB). To modify the code, fork the main repo into your own GitHub account or organization and commit changes there.

YCSB is written in Java (as most of the new cloud data stores at beginning of the project were written in Java) and is laid out as a multi-module Maven project. You should be able to import the project into your favorite IDE or environment easily. For more details about the Maven layout see the [Guide to Working with Multiple Modules](https://maven.apache.org/guides/mini/guide-multiple-modules.html).

## Licensing

YCSB is licensed under the Apache License, Version 2.0 (APL2). Every file included in the project must include the APL header. For example, each Java source file must have a header similar to the following:

```java
/**
 * Copyright (c) 2015-2017 YCSB contributors. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */ 
```

When modifying files that already have a license header, please update the year when you made your edits. E.g. change ``Copyright (c) 2010 Yahoo! Inc., 2012 - 2016 YCSB contributors.`` to ``Copyright (c) 2010 Yahoo! Inc., 2012 - 2017 YCSB contributors.`` If the file only has ``Copyright (c) 2010 Yahoo! Inc.``, append the current year as in ``Copyright (c) 2010 Yahoo! Inc., 2017 YCSB contributors.``.

**WARNING**: It should go without saying, but don't copy and paste code from outside authors or sources. If you are a database author and want to copy some example code, it must be APL2 compatible.

Client bindings to non-APL databases are perfectly acceptable, as data stores are meant to be used from all kinds of projects. Just make sure not to copy any code or commit libraries or binaries into the YCSB code base. Link to them in the Maven pom file.

## Issues and Support

To track bugs, feature requests and releases we use GitHub's integrated [Issues](https://github.com/brianfrankcooper/YCSB/issues). If you find a bug or problem, open an issue with a descriptive title and as many details as you can give us in the body (stack traces, log files, etc). Then if you can create a fix, follow the PR guidelines below.

**Note** Before embarking on a code change or DB, search through the existing issues and pull requests to see if anyone is already working on it. Reach out to them if so.

For general support, please use the mailing list hosted (of course) with Yahoo groups at [http://groups.yahoo.com/group/ycsb-users](http://groups.yahoo.com/group/ycsb-users).

## Code Style

A Java coding style guide is enforced via the Maven CheckStyle plugin. We try not to be too draconian with enforcement but the biggies include:

* Whitespaces instead of tabs.
* Proper Javadocs for methods and classes.
* Camel case member names.
* Upper camel case classes and method names.
* Line length.

CheckStyle will run for pull requests or if you create a package locally so if you just compile and push a commit, you may be surprised when the build fails with a style issue. Just execute ``mvn checkstyle:checkstyle `` before you open a PR and you should avoid any suprises.

## Platforms

Since most data bases aim to support multiple platforms, YCSB aims to run on as many as possible as well. Besides **Linux** and **macOS**, YCSB must compile and run for **Windows**. While not all DBs will run under every platform, the YCSB tool itself must be able to execute on all of these systems and hopefully be able to communicate with remote data stores.

Additionally, YCSB is targeting Java 7 (1.7.0) as its build version as some users are glacially slow moving to Java 8. So please avoid those Lambdas and Streams for now.

## Pull Requests

You've written some amazing code and are excited to share it with the community! It's time to open a PR! Here's what you should do.

* Checkout YCSB's ``master`` branch in your own fork and create a new branch based off of it with a name that is reflective of your work. E.g. ``i123`` for fixing an issue or ``db_xyz`` when working on a binding.
* Add your changes to the branch.
* Commit the code and start the commit message with the component you are working on in square braces. E.g. ``[core] Add another format for exporting histograms.`` or ``[hbase12] Fix interrupted exception bug.``.
* Push to your fork and click the ``Create Pull Request`` button.
* Wait for the build to complete in the CI pipeline. If it fails with a red X, click through the logs for details and fix any issues and commit your changes.
* If you have made changes, please flatten the commits so that the commit logs are nice and clean. Just run a ``git rebase -i <hash before your first commit>``. 

After you have opened your PR, a YCSB maintainer will review it and offer constructive feedback via the GitHub review feature. If no one has responded to your PR, please bump the thread by adding comments.

**NOTE**: For maintainers, please get another maintainer to sign off on your changes before merging a PR. And if you're writing code, please do create a PR from your fork, don't just push code directly to the master branch.

## Core, Bindings and Workloads

The main components of the code base include the core library and benchmarking utility, various database client bindings and workload classes and definitions.

### Core
When working on the core classes, keep in mind the following:

* Do not change the core behavior or operation of the main benchmarking classes (Particularly the Client and Workload classes). YCSB is used all over the place because it's a consistent standard that allows different users to compare results with the same workloads. If you find a way to drastically improve throughput, that's great! But please check with the rest of the maintainers to see if we can add the tweaks without invalidating years of benchmarks.
* Do not remove or modify measurements. Users may have tooling to parse the outputs so if you take something out, they'll be a wee bit unhappy. Extending or adding measurements is fine (so if you do have tooling, expect additions.)
* Do not modify existing generators. Again we don't want to invalidate years of benchmarks. Instead, create a new generator or option that can be enabled explicitly (not implicitly!) for users to try out.
* Utility classes and methods are welcome. But if they're only ever used by a specific database binding, co-locate the code with that binding.
* Don't change the DB interface if at all possible. Implementations can squeeze all kinds of workloads through the existing interface and while it may be easy to change the bindings included with the source code, some users may have private clients they can't share with the community. 

### Bindings and Clients

When a new database is released a *binding* can be created that implements a client communicating with the given data store that will execute YCSB workloads. Details about writing a DB binding can be found on our [GitHub Wiki page](https://github.com/brianfrankcooper/YCSB/wiki/Adding-a-Database). Some development guidelines to follow include:

* Create a new Maven module for your binding. Follow the existing bindings as examples.
* The module *must* include a README.md file with details such as:
  * Database setup with links to documentation so that the YCSB benchmarks will execute properly.
  * Example command line executions (workload selection, etc).
  * Required and optional properties (e.g. connection strings, behavior settings, etc) along with the default values.
  * Versions of the database the binding supports.
* Javadoc the binding and all of the methods. Tell us what it does and how it works.

Because YCSB is a utility to compare multiple data stores, we need each binding to behave similarly by default. That means each data store should enforce the strictest consistency guarantees available and avoid client side buffering or optimizations. This allows users to evaluate different DBs with a common baseline and tough standards.

However you *should* include parameters to tune and improve performance as much as possible to reach those flashy marketing numbers. Just be honest and document what the settings do and what trade-offs are made. (e.g. client side buffering reduces I/O but a crash can lead to data loss).

### Workloads

YCSB began comparing various key/value data stores with simple CRUD operations. However as DBs have become more specialized we've added more workloads for various tasks and would love to have more in the future. Keep the following in mind:

* Make sure more than one publicly available database can handle your workload. It's no fun if only one player is in the game.
* Use the existing DB interface to pass your data around. If you really need another API, discuss with the maintainers to see if there isn't a workaround.
* Provide real-world use cases for the workload, not just theoretical idealizations.