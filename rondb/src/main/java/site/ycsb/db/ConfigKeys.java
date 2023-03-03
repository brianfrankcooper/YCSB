/*
 * Copyright (c) 2023, Hopsworks AB. All rights reserved.
 *
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
package site.ycsb.db;

/**
 * Configuration keys for RonDB testing.
 */
public final class ConfigKeys {

  private  ConfigKeys(){}

  public static final String RONDB_API_TYPE_KEY = "rondb.api.type";
  public static final String RONDB_API_TYPE_DEFAULT = RonDBAPIType.CLUSTERJ.toString();

  public static final String SCHEMA_KEY = "rondb.schema";
  public static final String SCHEMA_DEFAULT = "ycsb";

  public static final String CONNECT_STR_KEY = "rondb.connection.string";
  public static final String CONNECT_STR_DEFAULT = "localhost:1186";

  public static final String RONDB_REST_SERVER_IP_KEY = "rondb.api.server.ip";
  public static final String RONDB_REST_SERVER_IP_DEFAULT = "localhost";

  public static final String RONDB_GRPC_SERVER_PORT_KEY = "rondb.api.server.grpc.port";
  public static final int RONDB_GRPC_SERVER_PORT_DEFAULT = 5406;

  public static final String RONDB_REST_SERVER_PORT_KEY = "rondb.api.server.rest.port";
  public static final int RONDB_REST_SERVER_PORT_DEFAULT = 4406;

  public static final String RONDB_REST_API_VERSION_KEY = "rondb.api.server.rest.version";
  public static final String RONDB_REST_API_VERSION_DEFAULT = "0.1.0";

  public static final String RONDB_REST_API_USE_ASYNC_REQUESTS_KEY = "rondb.rest.api.use.async.requests";
  public static final boolean RONDB_REST_API_USE_ASYNC_REQUESTS_DEFAULT = false;
}
