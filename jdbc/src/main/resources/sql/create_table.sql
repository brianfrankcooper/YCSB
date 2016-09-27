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

-- Creates a Table.

-- Drop the table if it exists;
DROP TABLE IF EXISTS usertable;

-- Create the user table with 5 fields.
CREATE TABLE usertable(YCSB_KEY VARCHAR PRIMARY KEY,
  FIELD0 VARCHAR, FIELD1 VARCHAR,
  FIELD2 VARCHAR, FIELD3 VARCHAR,
  FIELD4 VARCHAR, FIELD5 VARCHAR,
  FIELD6 VARCHAR, FIELD7 VARCHAR,
  FIELD8 VARCHAR, FIELD9 VARCHAR);
