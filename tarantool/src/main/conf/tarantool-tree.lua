-- Copyright (c) 2015 YCSB contributors. All rights reserved.
--
-- Licensed under the Apache License, Version 2.0 (the "License"); you
-- may not use this file except in compliance with the License. You
-- may obtain a copy of the License at
--
-- http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
-- implied. See the License for the specific language governing
-- permissions and limitations under the License. See accompanying
-- LICENSE file.

box.cfg {
   listen=3303,
   logger="tarantool.log",
   log_level=5,
   logger_nonblock=true,
   wal_mode="none",
   pid_file="tarantool.pid"
}

box.schema.space.create("ycsb", {id = 1024})
box.space.ycsb:create_index('primary', {type = 'tree', parts = {1, 'STR'}})
box.schema.user.grant('guest', 'read,write,execute', 'universe')
