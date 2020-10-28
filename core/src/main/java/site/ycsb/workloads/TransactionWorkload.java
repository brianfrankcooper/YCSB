/**
 * Copyright (c) 2010 Yahoo! Inc., Copyright (c) 2016-2017 YCSB contributors. All rights reserved.
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

package site.ycsb.workloads;

import site.ycsb.*;
import site.ycsb.generator.*;

import java.util.*;

import static site.ycsb.workloads.RestWorkload.DELETE_PROPORTION_PROPERTY;

/**
 * Extension of properties.
 * Properties added for this benchmark:
 * <UL>
 * <LI><b>startbalance</b>: the balance new insertions have (default: 1000)
 * <LI><b>transferproportion</b>: what proportion of operations should be transfers (default: 0)
 * <LI><b>mintransferamount</b>: what the minimal amount of transfer is (default: 1)
 * <LI><b>maxtransferamount</b>: what the max amount of transfer is (default: 1000)
 * <LI><b>deleteproportion</b>: what proportion of operations should be deletes (default: 0)
 * </UL>
 */
public class TransactionWorkload extends CoreWorkload {
  public static final String STARTBALANCE_PROPERTY = "startbalance";
  public static final String STARTBALANCE_PROPERTY_DEFAULT = "1000";
  public static final String MINTRANSFERAMOUNT_PROPERTY = "mintransferamount";
  public static final String MINTRANSFERAMOUNT_PROPERTY_DEFAULT = "1";
  public static final String MAXTRANSFERAMOUNT_PROPERTY = "maxtransferamount";
  public static final String MAXTRANSFERAMOUNT_PROPERTY_DEFAULT = "1000";
  public static final String TRANSFERAMOUNT_DISTRIBUTION_PROPERTY = "transferamountdistribution";
  /**
   * Uniform or Zipfian distributions.
   */
  public static final String TRANSFERAMOUNT_DISTRIBUTION_PROPERTY_DEFAULT = "uniform";
  public static final String TRANSFER_PROPORTION_PROPERTY = "transferproportion";
  public static final String TRANSFER_PROPORTION_PROPERTY_DEFAULT = "0.05";
  public static final String READ_PROPORTION_PROPERTY_DEFAULT = "0.90";
  public static final String DELETE_PROPORTION_PROPERTY = "deleteproportion";
  public static final String DELETE_PROPORTION_PROPERTY_DEFAULT = "0";

  protected NumberGenerator transferamount;
  protected int startbalance;

  /**
   * Initialize the scenario.
   * Called once, in the main client thread, before any operations are started.
   */
  @Override
  public void init(Properties p) throws WorkloadException {
    super.init(p);
    int mintransferamount =
        Integer.parseInt(p.getProperty(MINTRANSFERAMOUNT_PROPERTY, MINTRANSFERAMOUNT_PROPERTY_DEFAULT));
    int maxtransferamount =
        Integer.parseInt(p.getProperty(MAXTRANSFERAMOUNT_PROPERTY, MAXTRANSFERAMOUNT_PROPERTY_DEFAULT));

    String transferAmountDistribution = p.getProperty(TRANSFERAMOUNT_DISTRIBUTION_PROPERTY,
        TRANSFERAMOUNT_DISTRIBUTION_PROPERTY_DEFAULT);
    if (transferAmountDistribution.compareTo("uniform") == 0) {
      transferamount = new UniformLongGenerator(mintransferamount, maxtransferamount);
    } else if (transferAmountDistribution.compareTo("zipfian") == 0) {
      transferamount = new ZipfianGenerator(mintransferamount, maxtransferamount);
    } else {
      throw new WorkloadException("Unknown request distribution \"" + transferAmountDistribution + "\"");
    }
    startbalance = Integer.parseInt(p.getProperty(STARTBALANCE_PROPERTY, STARTBALANCE_PROPERTY_DEFAULT));
  }

  /**
   * Do one insert operation. Because it will be called concurrently from multiple client threads,
   * this function must be thread safe. However, avoid synchronized, or the threads will block waiting
   * for each other, and it will be difficult to reach the target throughput. Ideally, this function would
   * have no side effects other than DB operations.
   */
  @Override
  public boolean doInsert(DB db, Object threadstate) {
    int keynum = keysequence.nextValue().intValue();
    String dbkey = buildKeyName(keynum);
    HashMap<String, ByteIterator> values = super.buildValues(dbkey);

    Status status;
    int numOfRetries = 0;
    do {
      status = db.insert(table, dbkey, values, startbalance);
      if (null != status && status.isOk()) {
        break;
      }
      // Retry if configured. Without retrying, the load process will fail
      // even if one single insertion fails. User can optionally configure
      // an insertion retry limit (default is 0) to enable retry.
      if (++numOfRetries <= insertionRetryLimit) {
        System.err.println("Retrying insertion, retry count: " + numOfRetries);
        try {
          // Sleep for a random number between [0.8, 1.2)*insertionRetryInterval.
          int sleepTime = (int) (1000 * insertionRetryInterval * (0.8 + 0.4 * Math.random()));
          Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
          break;
        }

      } else {
        System.err.println("Error inserting, not retrying any more. number of attempts: " + numOfRetries +
            "Insertion Retry Limit: " + insertionRetryLimit);
        break;

      }
    } while (true);

    return null != status && status.isOk();
  }

  /**
   * Do one transaction operation. Because it will be called concurrently from multiple client
   * threads, this function must be thread safe. However, avoid synchronized, or the threads will block waiting
   * for each other, and it will be difficult to reach the target throughput. Ideally, this function would
   * have no side effects other than Transaction operations.
   */
  @Override
  public boolean doTransaction(DB db, Object threadstate) {
    String operation = operationchooser.nextString();
    if(operation == null) {
      return false;
    }

    switch (operation) {
    case "READ":
      doTransactionRead(db);
      break;
    case "UPDATE":
      doTransactionUpdate(db);
      break;
    case "TRANSFER":
      doTransactionTransfer(db);
      break;
    case "INSERT":
      doTransactionInsert(db);
      break;
    case "DELETE":
      doTransactionDeleteAndTransfer(db);
      break;
    case "SCAN":
      doTransactionScan(db);
      break;
    default:
      doTransactionReadModifyWrite(db);
    }

    return true;
  }

  private void doTransactionTransfer(DB db) {
    long keynum = nextKeynum();
    long keynumTwo = keynum;
    while (keynum == keynumTwo) {
      keynumTwo = nextKeynum();
    }
    db.transfer(table, buildKeyName(keynum),
        buildKeyName(keynumTwo), transferamount.nextValue().intValue());
  };

  public void doTransactionRead(DB db) {
    // choose a random key
    long keynum = nextKeynum();

    String keyname = buildKeyName(keynum);

    HashSet<String> fields = null;

    if (!readallfields) {
      // read a random field
      String fieldname = fieldnames.get(fieldchooser.nextValue().intValue());

      fields = new HashSet<String>();
      fields.add(fieldname);
    } else if (dataintegrity) {
      // pass the full field list if dataintegrity is on for verification
      fields = new HashSet<String>(fieldnames);
    }

    HashMap<String, ByteIterator> cells = new HashMap<String, ByteIterator>();
    db.read(table, keyname, fields, cells);

    if (dataintegrity) {
      verifyRow(keyname, cells);
    }
  }

  public void doTransactionDeleteAndTransfer(DB db) {
    long keynum = nextKeynum();
    long keynumTwo = keynum;
    while (keynum == keynumTwo) {
      keynumTwo = nextKeynum();
    }
    db.deleteAndTransfer(table, buildKeyName(keynum), buildKeyName(keynumTwo));
  }

  public void doTransactionInsert(DB db) {
    // choose the next key
    long keynum = transactioninsertkeysequence.nextValue();

    try {
      String dbkey = buildKeyName(keynum);

      HashMap<String, ByteIterator> values = buildValues(dbkey);
      db.insert(table, dbkey, values, startbalance);
    } finally {
      transactioninsertkeysequence.acknowledge(keynum);
    }
  }

  /**
   * Creates a weighted discrete values with database operations for a workload to perform.
   * Weights/proportions are read from the properties list and defaults are used
   * when values are not configured.
   * Current operations are "READ", "UPDATE", "INSERT", "SCAN" and "READMODIFYWRITE".
   *
   * @param p The properties list to pull weights from.
   * @return A generator that can be used to determine the next operation to perform.
   * @throws IllegalArgumentException if the properties object was null.
   */
  protected static DiscreteGenerator createOperationGenerator(final Properties p) {
    if (p == null) {
      throw new IllegalArgumentException("Properties object cannot be null");
    }
    final double readproportion = Double.parseDouble(
        p.getProperty(READ_PROPORTION_PROPERTY, READ_PROPORTION_PROPERTY_DEFAULT));
    final double updateproportion = Double.parseDouble(
        p.getProperty(UPDATE_PROPORTION_PROPERTY, UPDATE_PROPORTION_PROPERTY_DEFAULT));
    final double insertproportion = Double.parseDouble(
        p.getProperty(INSERT_PROPORTION_PROPERTY, INSERT_PROPORTION_PROPERTY_DEFAULT));
    final double transferproportion = Double.parseDouble(
        p.getProperty(TRANSFER_PROPORTION_PROPERTY, TRANSFER_PROPORTION_PROPERTY_DEFAULT));
    final double deleteproportion = Double.parseDouble(
        p.getProperty(DELETE_PROPORTION_PROPERTY, DELETE_PROPORTION_PROPERTY_DEFAULT));
    final double scanproportion = Double.parseDouble(
        p.getProperty(SCAN_PROPORTION_PROPERTY, SCAN_PROPORTION_PROPERTY_DEFAULT));
    final double readmodifywriteproportion = Double.parseDouble(p.getProperty(
        READMODIFYWRITE_PROPORTION_PROPERTY, READMODIFYWRITE_PROPORTION_PROPERTY_DEFAULT));

    final DiscreteGenerator operationchooser = new DiscreteGenerator();
    if (readproportion > 0) {
      operationchooser.addValue(readproportion, "READ");
    }

    if (updateproportion > 0) {
      operationchooser.addValue(updateproportion, "UPDATE");
    }

    if (transferproportion > 0) {
      operationchooser.addValue(transferproportion, "TRANSFER");
    }

    if (deleteproportion > 0) {
      operationchooser.addValue(deleteproportion, "DELETE");
    }

    if (insertproportion > 0) {
      operationchooser.addValue(insertproportion, "INSERT");
    }

    if (scanproportion > 0) {
      operationchooser.addValue(scanproportion, "SCAN");
    }

    if (readmodifywriteproportion > 0) {
      operationchooser.addValue(readmodifywriteproportion, "READMODIFYWRITE");
    }
    return operationchooser;
  }
}
