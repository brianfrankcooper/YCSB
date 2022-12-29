/*
 * Copyright (c) 2022, Yahoo!, Inc. All rights reserved.
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

package site.ycsb.db.clusterj.tx;

import com.mysql.clusterj.ClusterJException;
import com.mysql.clusterj.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import site.ycsb.Status;

/**
 * Transaction Request Handlder.
 */

public abstract class TransactionReqHandler {
  private String name = "";

  private static Logger logger = LoggerFactory.getLogger(TransactionReqHandler.class);

  public TransactionReqHandler(String name) {
    this.name = name;
  }

  public abstract Status action() throws Exception;

  public Status runTx(Session session, Class<?> c, Object partKey) {

    boolean txCompleted = false;
    try {
      if (session.currentTransaction().isActive()) {
        session.currentTransaction().rollback();
      }
      session.currentTransaction().begin();
      session.setPartitionKey(c, partKey);
      Status retVal = action();
      session.currentTransaction().commit();
      txCompleted = true;
      return retVal;

    } catch (Exception e) {
      Status retStatus;
      logger.warn(name + " Error: " + e);
      if (isSessionClosing(e)) {
        retStatus = Status.OK;
      } else {
        logger.warn(name + " Error: " + e);
        retStatus = Status.ERROR;
      }

      try {
        if (!txCompleted) {
          if (session.currentTransaction().isActive()) {
            session.currentTransaction().rollback();
          }
        }
      } catch (Exception e2) {
        logger.warn(name + " Error: " + e2);
      }
      return retStatus;
    }
  }

  private boolean isSessionClosing(Exception e) {
    if (e instanceof ClusterJException && e.getMessage().contains("Db is closing")) {
      return true;
    }
    return false;
  }

}
