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
CREATE TABLE usertable(YCSB_KEY string PRIMARY KEY,
  zFIELD01 string, zFIELD02 string,
  zFIELD03 string, zFIELD04 string,
  zFIELD05 string, zFIELD06 string,
  zFIELD07 string, zFIELD08 string,
  zFIELD09 string, zFIELD10 string);
