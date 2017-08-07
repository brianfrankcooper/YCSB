/**
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package com.yahoo.ycsb.db.hbase12;

/**
 * HBase 1.2 client for YCSB framework.
 *
 * A modified version of HBaseClient (which targets HBase v1.2) utilizing the
 * shaded client.
 *
 * It should run equivalent to following the hbase098 binding README.
 *
 */
public class HBaseClient12 extends com.yahoo.ycsb.db.HBaseClient10 {
}
