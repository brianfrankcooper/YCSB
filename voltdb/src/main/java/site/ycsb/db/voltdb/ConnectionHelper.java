/**
 * Copyright (c) 2015-2019 YCSB contributors. All rights reserved.
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

/*
 * VoltDB Connection Utility.
 */
package site.ycsb.db.voltdb;

import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;

/**
 * Help class to create VoltDB connections for YCSB benchmark.
 */
public final class ConnectionHelper {
  
  /**
   * Default port for VoltDB.
   */
  private static final int VOLTDB_DEFAULT_PORT = 21212;

  /**
   * hidden constructor.
   */
  private ConnectionHelper() {

  }

  /**
   * Creates a factory used to connect to a VoltDB instance. (Note that if a
   * corresponding connection exists, all parameters other than 'servers' are
   * ignored)
   * 
   * @param servers   The comma separated list of VoltDB servers in
   *                  hostname[:port] format that the instance will use.
   * @param user      The username for the connection
   * @param password  The password for the specified user
   * @param ratelimit A limit on the number of transactions per second for the
   *                  VoltDB instance
   * @return The existing factory if a corresponding connection has already been
   *         created; the newly created one otherwise.
   * @throws IOException          Throws if a connection is already open with a
   *                              different server string.
   * @throws InterruptedException
   */
  public static Client createConnection(String servers, String user, String password,
      int ratelimit) throws IOException, InterruptedException {
 
    ClientConfig config = new ClientConfig(user, password);
    config.setMaxTransactionsPerSecond(ratelimit);
    Client client = ClientFactory.createClient(config);
    
    // Note that in VoltDB there is a distinction between creating an instance of a client
    // and actually connecting to the DB...
    connect(client, servers);
    
    return client;
  }


  /**
   * Connect to a single server with retry. Limited exponential backoff. No
   * timeout. This will run until the process is killed if it's not able to
   * connect.
   *
   * @param server hostname:port or just hostname (hostname can be ip).
   */
  private static void connectToOneServerWithRetry(final Client client, String server) {
    
    Logger logger = LoggerFactory.getLogger(ConnectionHelper.class);
           
    int sleep = 1000;
    while (true) {
      try {
        client.createConnection(server);
        break;
      } catch (Exception e) {
        logger.error("Connection failed - retrying in %d second(s).\n", sleep / 1000);
        try {
          Thread.sleep(sleep);
        } catch (java.lang.InterruptedException e2) {
          logger.error(e2.getMessage());
        }
        if (sleep < 8000) {
          sleep += sleep;
        }
      }
    }
    
    logger.info("Connected to VoltDB node at:" + server);
  }

  /**
   * See if DB servers are present on the network.
   * 
   * @return true or false
   */
  public static boolean checkDBServers(String servernames) {

    String[] serverNamesArray = servernames.split(",");

    boolean dbThere = false;

    Socket socket = null;
    try {
      // Connect
      socket = new Socket(serverNamesArray[0], VOLTDB_DEFAULT_PORT);
      dbThere = true;
    } catch (IOException connectFailed) {
      dbThere = false;
    } finally {
      if (socket != null) {
        try {
          socket.close();
        } catch (IOException ignore) {
          // Ignore.
        }
      }
      socket = null;
    }

    return dbThere;

  }

  /**
   * Connect to a set of servers in parallel. Each will retry until connection.
   * This call will block until all have connected.
   *
   * @param servers A comma separated list of servers using the hostname:port
   *                syntax (where :port is optional).
   * @throws InterruptedException if anything bad happens with the threads.
   */
  private static void connect(final Client client, String servers) throws InterruptedException {
    
    Logger logger = LoggerFactory.getLogger(ConnectionHelper.class);
    
    logger.info("Connecting to VoltDB...");

    String[] serverArray = servers.split(",");
    final CountDownLatch connections = new CountDownLatch(serverArray.length);

    // use a new thread to connect to each server
    for (final String server : serverArray) {
      new Thread(new Runnable() {
        @Override
        public void run() {
          connectToOneServerWithRetry(client, server);
          connections.countDown();
        }
      }).start();
    }
    // block until all have connected
    connections.await();
  }


}
