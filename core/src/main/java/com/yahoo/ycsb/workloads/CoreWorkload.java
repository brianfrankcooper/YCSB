/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
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

package com.yahoo.ycsb.workloads;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.Client;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.RandomByteIterator;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;
import com.yahoo.ycsb.Utils;
import com.yahoo.ycsb.Workload;
import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.generator.AcknowledgedCounterGenerator;
import com.yahoo.ycsb.generator.ConstantIntegerGenerator;
import com.yahoo.ycsb.generator.CounterGenerator;
import com.yahoo.ycsb.generator.DiscreteGenerator;
import com.yahoo.ycsb.generator.ExponentialGenerator;
import com.yahoo.ycsb.generator.Generator;
import com.yahoo.ycsb.generator.HistogramGenerator;
import com.yahoo.ycsb.generator.HotspotIntegerGenerator;
import com.yahoo.ycsb.generator.IntegerGenerator;
import com.yahoo.ycsb.generator.ScrambledZipfianGenerator;
import com.yahoo.ycsb.generator.SkewedLatestGenerator;
import com.yahoo.ycsb.generator.UniformIntegerGenerator;
import com.yahoo.ycsb.generator.ZipfianGenerator;
import com.yahoo.ycsb.measurements.Measurements;

/**
 * The core benchmark scenario. Represents a set of clients doing simple CRUD
 * operations. The relative proportion of different kinds of operations, and
 * other properties of the workload, are controlled by parameters specified at
 * runtime.
 *
 * Properties to control the client:
 * <UL>
 * <LI><b>fieldcount</b>: the number of fields in a record (default: 10)
 * <LI><b>fieldlength</b>: the size of each field (default: 100)
 * <LI><b>readallfields</b>: should reads read all fields (true) or just one
 * (false) (default: true)
 * <LI><b>writeallfields</b>: should updates and read/modify/writes update all
 * fields (true) or just one (false) (default: false)
 * <LI><b>readproportion</b>: what proportion of operations should be reads
 * (default: 0.95)
 * <LI><b>updateproportion</b>: what proportion of operations should be updates
 * (default: 0.05)
 * <LI><b>insertproportion</b>: what proportion of operations should be inserts
 * (default: 0)
 * <LI><b>scanproportion</b>: what proportion of operations should be scans
 * (default: 0)
 * <LI><b>readmodifywriteproportion</b>: what proportion of operations should be
 * read a record, modify it, write it back (default: 0)
 * <LI><b>requestdistribution</b>: what distribution should be used to select
 * the records to operate on - uniform, zipfian, hotspot, or latest (default:
 * uniform)
 * <LI><b>maxscanlength</b>: for scans, what is the maximum number of records to
 * scan (default: 1000)
 * <LI><b>scanlengthdistribution</b>: for scans, what distribution should be
 * used to choose the number of records to scan, for each scan, between 1 and
 * maxscanlength (default: uniform)
 * <LI><b>insertorder</b>: should records be inserted in order by key
 * ("ordered"), or in hashed order ("hashed") (default: hashed)
 * </ul>
 */
public class CoreWorkload extends Workload {

  /**
   * The name of the property for deciding whether to check all returned data
   * against the formation template to ensure data integrity.
   */
  public static final String DATA_INTEGRITY_PROPERTY = "dataintegrity";

  /**
   * The default value for the dataintegrity property.
   */
  public static final String DATA_INTEGRITY_PROPERTY_DEFAULT = "false";

  /**
   * The name of the property for the number of fields in a record.
   */
  public static final String FIELD_COUNT_PROPERTY = "fieldcount";

  /**
   * Default number of fields in a record.
   */
  public static final String FIELD_COUNT_PROPERTY_DEFAULT = "10";

  /**
   * The name of the property for the field length distribution. Options are
   * "uniform", "zipfian" (favoring short records), "constant", and "histogram".
   * 
   * If "uniform", "zipfian" or "constant", the maximum field length will be
   * that specified by the fieldlength property. If "histogram", then the
   * histogram will be read from the filename specified in the
   * "fieldlengthhistogram" property.
   */
  public static final String FIELD_LENGTH_DISTRIBUTION_PROPERTY =
      "fieldlengthdistribution";

  /**
   * The default field length distribution.
   */
  public static final String FIELD_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT =
      "constant";

  /**
   * The name of a property that specifies the filename containing the field
   * length histogram (only used if fieldlengthdistribution is "histogram").
   */
  public static final String FIELD_LENGTH_HISTOGRAM_FILE_PROPERTY =
      "fieldlengthhistogram";

  /**
   * The default filename containing a field length histogram.
   */
  public static final String FIELD_LENGTH_HISTOGRAM_FILE_PROPERTY_DEFAULT =
      "hist.txt";
  /**
   * The name of the property for the length of a field in bytes.
   */
  public static final String FIELD_LENGTH_PROPERTY = "fieldlength";

  /**
   * The default maximum length of a field in bytes.
   */
  public static final String FIELD_LENGTH_PROPERTY_DEFAULT = "100";
  /**
   * Percentage data items that constitute the hot set.
   */
  public static final String HOTSPOT_DATA_FRACTION = "hotspotdatafraction";

  /**
   * Default value of the size of the hot set.
   */
  public static final String HOTSPOT_DATA_FRACTION_DEFAULT = "0.2";
  /**
   * Percentage operations that access the hot set.
   */
  public static final String HOTSPOT_OPN_FRACTION = "hotspotopnfraction";

  /**
   * Default value of the percentage operations accessing the hot set.
   */
  public static final String HOTSPOT_OPN_FRACTION_DEFAULT = "0.8";

  /**
   * The name of the property for the order to insert records. Options are
   * "ordered" or "hashed"
   */
  public static final String INSERT_ORDER_PROPERTY = "insertorder";

  /**
   * Default insert order.
   */
  public static final String INSERT_ORDER_PROPERTY_DEFAULT = "hashed";

  /**
   * The name of the property for the proportion of transactions that are
   * inserts.
   */
  public static final String INSERT_PROPORTION_PROPERTY = "insertproportion";

  /**
   * The default proportion of transactions that are inserts.
   */
  public static final String INSERT_PROPORTION_PROPERTY_DEFAULT = "0.0";

  /**
   * The name of the property for the max scan length (number of records).
   */
  public static final String MAX_SCAN_LENGTH_PROPERTY = "maxscanlength";

  /**
   * The default max scan length.
   */
  public static final String MAX_SCAN_LENGTH_PROPERTY_DEFAULT = "1000";

  /**
   * The name of the property for deciding whether to read one field (false) or
   * all fields (true) of a record.
   */
  public static final String READ_ALL_FIELDS_PROPERTY = "readallfields";

  /**
   * The default value for the readallfields property.
   */
  public static final String READ_ALL_FIELDS_PROPERTY_DEFAULT = "true";

  /**
   * The name of the property for the proportion of transactions that are reads.
   */
  public static final String READ_PROPORTION_PROPERTY = "readproportion";

  /**
   * The default proportion of transactions that are reads.
   */
  public static final String READ_PROPORTION_PROPERTY_DEFAULT = "0.95";
  /**
   * The name of the property for the proportion of transactions that are
   * read-modify-write.
   */
  public static final String READMODIFYWRITE_PROPORTION_PROPERTY =
      "readmodifywriteproportion";
  /**
   * The default proportion of transactions that are scans.
   */
  public static final String READMODIFYWRITE_PROPORTION_PROPERTY_DEFAULT =
      "0.0";

  /**
   * The name of the property for the the distribution of requests across the
   * keyspace. Options are "uniform", "zipfian" and "latest"
   */
  public static final String REQUEST_DISTRIBUTION_PROPERTY =
      "requestdistribution";

  /**
   * The default distribution of requests across the keyspace.
   */
  public static final String REQUEST_DISTRIBUTION_PROPERTY_DEFAULT = "uniform";

  /**
   * The name of the property for the scan length distribution. Options are
   * "uniform" and "zipfian" (favoring short scans)
   */
  public static final String SCAN_LENGTH_DISTRIBUTION_PROPERTY =
      "scanlengthdistribution";

  /**
   * The default max scan length.
   */
  public static final String SCAN_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT =
      "uniform";

  /**
   * The name of the property for the proportion of transactions that are scans.
   */
  public static final String SCAN_PROPORTION_PROPERTY = "scanproportion";

  /**
   * The default proportion of transactions that are scans.
   */
  public static final String SCAN_PROPORTION_PROPERTY_DEFAULT = "0.0";

  /**
   * The name of the database table to run queries against.
   */
  public static final String TABLENAME_PROPERTY = "table";

  /**
   * The default name of the database table to run queries against.
   */
  public static final String TABLENAME_PROPERTY_DEFAULT = "usertable";

  /**
   * The name of the property for the proportion of transactions that are
   * updates.
   */
  public static final String UPDATE_PROPORTION_PROPERTY = "updateproportion";

  /**
   * The default proportion of transactions that are updates.
   */
  public static final String UPDATE_PROPORTION_PROPERTY_DEFAULT = "0.05";

  /**
   * The name of the property for deciding whether to write one field (false) or
   * all fields (true) of a record.
   */
  public static final String WRITE_ALL_FIELDS_PROPERTY = "writeallfields";

  /**
   * The default value for the writeallfields property.
   */
  public static final String WRITE_ALL_FIELDS_PROPERTY_DEFAULT = "false";

  private static final int DATA_INT_DEVIATE = 1000;

  /**
   * Response values for data integrity checks. Need to be multiples of 1000 to
   * match bucket offsets of measurements/OneMeasurementHistogram.java.
   */
  private static final int DATA_INT_MATCH = 0;

  private static final int DATA_INT_UNEXPECTED_NULL = 2000;

  protected static IntegerGenerator getFieldLengthGenerator(final Properties p)
      throws WorkloadException {
    IntegerGenerator fieldlengthgenerator;
    final String fieldlengthdistribution =
        p.getProperty(FIELD_LENGTH_DISTRIBUTION_PROPERTY,
            FIELD_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT);
    final int fieldlength = Integer.parseInt(
        p.getProperty(FIELD_LENGTH_PROPERTY, FIELD_LENGTH_PROPERTY_DEFAULT));
    final String fieldlengthhistogram =
        p.getProperty(FIELD_LENGTH_HISTOGRAM_FILE_PROPERTY,
            FIELD_LENGTH_HISTOGRAM_FILE_PROPERTY_DEFAULT);
    if (fieldlengthdistribution.compareTo("constant") == 0) {
      fieldlengthgenerator = new ConstantIntegerGenerator(fieldlength);
    } else if (fieldlengthdistribution.compareTo("uniform") == 0) {
      fieldlengthgenerator = new UniformIntegerGenerator(1, fieldlength);
    } else if (fieldlengthdistribution.compareTo("zipfian") == 0) {
      fieldlengthgenerator = new ZipfianGenerator(1, fieldlength);
    } else if (fieldlengthdistribution.compareTo("histogram") == 0) {
      try {
        fieldlengthgenerator = new HistogramGenerator(fieldlengthhistogram);
      } catch (final IOException e) {
        throw new WorkloadException(
            "Couldn't read field length histogram file: "
                + fieldlengthhistogram,
            e);
      }
    } else {
      throw new WorkloadException("Unknown field length distribution \""
          + fieldlengthdistribution + "\"");
    }
    return fieldlengthgenerator;
  }

  private final Measurements measurements = Measurements.getMeasurements();

  /**
   * Set to true if want to check correctness of reads. Must also be set to true
   * during loading phase to function.
   */
  private boolean dataintegrity;

  private Generator fieldchooser;

  private int fieldcount;

  /**
   * Generator object that produces field lengths. The value of this depends on
   * the properties that start with "FIELD_LENGTH_".
   */
  private IntegerGenerator fieldlengthgenerator;

  private List<String> fieldnames;

  private IntegerGenerator keychooser;

  private IntegerGenerator keysequence;

  private DiscreteGenerator operationchooser;

  private boolean orderedinserts;

  private boolean readallfields;

  private int recordcount;

  private IntegerGenerator scanlength;

  private String table;

  private AcknowledgedCounterGenerator transactioninsertkeysequence;

  private boolean writeallfields;

  public String buildKeyName(long keynum) {
    if (!orderedinserts) {
      keynum = Utils.hash(keynum);
    }
    return "user" + keynum;
  }

  /**
   * Do one insert operation. Because it will be called concurrently from
   * multiple client threads, this function must be thread safe. However, avoid
   * synchronized, or the threads will block waiting for each other, and it will
   * be difficult to reach the target throughput. Ideally, this function would
   * have no side effects other than DB operations.
   */
  @Override
  public boolean doInsert(final DB db, final Object threadstate) {
    final int keynum = keysequence.nextInt();
    final String dbkey = buildKeyName(keynum);
    final HashMap<String, ByteIterator> values = buildValues(dbkey);
    
    return (db.insert(table, dbkey, values).equals(Status.OK));
  }

  /**
   * Do one transaction operation. Because it will be called concurrently from
   * multiple client threads, this function must be thread safe. However, avoid
   * synchronized, or the threads will block waiting for each other, and it will
   * be difficult to reach the target throughput. Ideally, this function would
   * have no side effects other than DB operations.
   */
  @Override
  public boolean doTransaction(final DB db, final Object threadstate) {
    final String op = operationchooser.nextString();

    if (op.compareTo("READ") == 0) {
      doTransactionRead(db);
    } else if (op.compareTo("UPDATE") == 0) {
      doTransactionUpdate(db);
    } else if (op.compareTo("INSERT") == 0) {
      doTransactionInsert(db);
    } else if (op.compareTo("SCAN") == 0) {
      doTransactionScan(db);
    } else {
      doTransactionReadModifyWrite(db);
    }

    return true;
  }

  public void doTransactionInsert(final DB db) {
    // choose the next key
    final int keynum = transactioninsertkeysequence.nextInt();

    try {
      final String dbkey = buildKeyName(keynum);

      final HashMap<String, ByteIterator> values = buildValues(dbkey);
      db.insert(table, dbkey, values);
    } finally {
      transactioninsertkeysequence.acknowledge(keynum);
    }
  }

  public void doTransactionRead(final DB db) {
    // choose a random key
    final int keynum = nextKeynum();

    final String keyname = buildKeyName(keynum);

    HashSet<String> fields = null;

    if (!readallfields) {
      // read a random field
      final String fieldname =
          fieldnames.get(Integer.parseInt(fieldchooser.nextString()));

      fields = new HashSet<String>();
      fields.add(fieldname);
    }

    final HashMap<String, ByteIterator> cells =
        new HashMap<String, ByteIterator>();
    db.read(table, keyname, fields, cells);

    if (dataintegrity) {
      verifyRow(keyname, cells);
    }
  }

  public void doTransactionReadModifyWrite(final DB db) {
    // choose a random key
    final int keynum = nextKeynum();

    final String keyname = buildKeyName(keynum);

    HashSet<String> fields = null;

    if (!readallfields) {
      // read a random field
      final String fieldname =
          fieldnames.get(Integer.parseInt(fieldchooser.nextString()));

      fields = new HashSet<String>();
      fields.add(fieldname);
    }

    HashMap<String, ByteIterator> values;

    if (writeallfields) {
      // new data for all the fields
      values = buildValues(keyname);
    } else {
      // update a random field
      values = buildSingleValue(keyname);
    }

    // do the transaction

    final HashMap<String, ByteIterator> cells =
        new HashMap<String, ByteIterator>();

    final long ist = measurements.getIntendedtartTimeNs();
    final long st = System.nanoTime();
    db.read(table, keyname, fields, cells);

    db.update(table, keyname, values);

    final long en = System.nanoTime();

    if (dataintegrity) {
      verifyRow(keyname, cells);
    }

    measurements.measure("READ-MODIFY-WRITE", (int) ((en - st) / 1000));
    measurements.measureIntended("READ-MODIFY-WRITE",
        (int) ((en - ist) / 1000));
  }

  public void doTransactionScan(final DB db) {
    // choose a random key
    final int keynum = nextKeynum();

    final String startkeyname = buildKeyName(keynum);

    // choose a random scan length
    final int len = scanlength.nextInt();

    HashSet<String> fields = null;

    if (!readallfields) {
      // read a random field
      final String fieldname =
          fieldnames.get(Integer.parseInt(fieldchooser.nextString()));

      fields = new HashSet<String>();
      fields.add(fieldname);
    }

    db.scan(table, startkeyname, len, fields,
        new Vector<HashMap<String, ByteIterator>>());
  }

  public void doTransactionUpdate(final DB db) {
    // choose a random key
    final int keynum = nextKeynum();

    final String keyname = buildKeyName(keynum);

    HashMap<String, ByteIterator> values;

    if (writeallfields) {
      // new data for all the fields
      values = buildValues(keyname);
    } else {
      // update a random field
      values = buildSingleValue(keyname);
    }

    db.update(table, keyname, values);
  }

  /**
   * Initialize the scenario. Called once, in the main client thread, before any
   * operations are started.
   */
  @Override
  public void init(final Properties p) throws WorkloadException {
    table = p.getProperty(TABLENAME_PROPERTY, TABLENAME_PROPERTY_DEFAULT);

    fieldcount = Integer.parseInt(
        p.getProperty(FIELD_COUNT_PROPERTY, FIELD_COUNT_PROPERTY_DEFAULT));
    fieldnames = new ArrayList<String>();
    for (int i = 0; i < fieldcount; i++) {
      fieldnames.add("field" + i);
    }
    fieldlengthgenerator = CoreWorkload.getFieldLengthGenerator(p);

    final double readproportion =
        Double.parseDouble(p.getProperty(READ_PROPORTION_PROPERTY,
            READ_PROPORTION_PROPERTY_DEFAULT));
    final double updateproportion =
        Double.parseDouble(p.getProperty(UPDATE_PROPORTION_PROPERTY,
            UPDATE_PROPORTION_PROPERTY_DEFAULT));
    final double insertproportion =
        Double.parseDouble(p.getProperty(INSERT_PROPORTION_PROPERTY,
            INSERT_PROPORTION_PROPERTY_DEFAULT));
    final double scanproportion =
        Double.parseDouble(p.getProperty(SCAN_PROPORTION_PROPERTY,
            SCAN_PROPORTION_PROPERTY_DEFAULT));
    final double readmodifywriteproportion =
        Double.parseDouble(p.getProperty(READMODIFYWRITE_PROPORTION_PROPERTY,
            READMODIFYWRITE_PROPORTION_PROPERTY_DEFAULT));
    recordcount = Integer.parseInt(p.getProperty(Client.RECORD_COUNT_PROPERTY,
        Client.DEFAULT_RECORD_COUNT));
    if (recordcount == 0) {
      recordcount = Integer.MAX_VALUE;
    }
    final String requestdistrib = p.getProperty(REQUEST_DISTRIBUTION_PROPERTY,
        REQUEST_DISTRIBUTION_PROPERTY_DEFAULT);
    final int maxscanlength =
        Integer.parseInt(p.getProperty(MAX_SCAN_LENGTH_PROPERTY,
            MAX_SCAN_LENGTH_PROPERTY_DEFAULT));
    final String scanlengthdistrib =
        p.getProperty(SCAN_LENGTH_DISTRIBUTION_PROPERTY,
            SCAN_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT);

    final int insertstart = Integer.parseInt(
        p.getProperty(INSERT_START_PROPERTY, INSERT_START_PROPERTY_DEFAULT));

    readallfields = Boolean.parseBoolean(p.getProperty(READ_ALL_FIELDS_PROPERTY,
        READ_ALL_FIELDS_PROPERTY_DEFAULT));
    writeallfields =
        Boolean.parseBoolean(p.getProperty(WRITE_ALL_FIELDS_PROPERTY,
            WRITE_ALL_FIELDS_PROPERTY_DEFAULT));

    dataintegrity = Boolean.parseBoolean(p.getProperty(DATA_INTEGRITY_PROPERTY,
        DATA_INTEGRITY_PROPERTY_DEFAULT));
    // Confirm that fieldlengthgenerator returns a constant if data
    // integrity check requested.
    if (dataintegrity && !(p.getProperty(FIELD_LENGTH_DISTRIBUTION_PROPERTY,
        FIELD_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT)).equals("constant")) {
      System.err
          .println("Must have constant field size to check data integrity.");
      System.exit(-1);
    }

    if (p.getProperty(INSERT_ORDER_PROPERTY, INSERT_ORDER_PROPERTY_DEFAULT)
        .compareTo("hashed") == 0) {
      orderedinserts = false;
    } else if (requestdistrib.compareTo("exponential") == 0) {
      final double percentile = Double.parseDouble(
          p.getProperty(ExponentialGenerator.EXPONENTIAL_PERCENTILE_PROPERTY,
              ExponentialGenerator.EXPONENTIAL_PERCENTILE_DEFAULT));
      final double frac = Double.parseDouble(
          p.getProperty(ExponentialGenerator.EXPONENTIAL_FRAC_PROPERTY,
              ExponentialGenerator.EXPONENTIAL_FRAC_DEFAULT));
      keychooser = new ExponentialGenerator(percentile, recordcount * frac);
    } else {
      orderedinserts = true;
    }

    keysequence = new CounterGenerator(insertstart);
    operationchooser = new DiscreteGenerator();
    if (readproportion > 0) {
      operationchooser.addValue(readproportion, "READ");
    }

    if (updateproportion > 0) {
      operationchooser.addValue(updateproportion, "UPDATE");
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

    transactioninsertkeysequence =
        new AcknowledgedCounterGenerator(recordcount);
    if (requestdistrib.compareTo("uniform") == 0) {
      keychooser = new UniformIntegerGenerator(0, recordcount - 1);
    } else if (requestdistrib.compareTo("zipfian") == 0) {
      // it does this by generating a random "next key" in part by taking the
      // modulus over the number of keys
      // if the number of keys changes, this would shift the modulus, and we
      // don't want that to change which keys are popular
      // so we'll actually construct the scrambled zipfian generator with a
      // keyspace that is larger than exists at the beginning
      // of the test. that is, we'll predict the number of inserts, and tell the
      // scrambled zipfian generator the number of existing keys
      // plus the number of predicted keys as the total keyspace. then, if the
      // generator picks a key that hasn't been inserted yet, will
      // just ignore it and pick another key. this way, the size of the keyspace
      // doesn't change from the perspective of the scrambled zipfian generator

      final int opcount =
          Integer.parseInt(p.getProperty(Client.OPERATION_COUNT_PROPERTY));
      // 2 is fudge factor.
      final int expectednewkeys = (int) ((opcount) * insertproportion * 2.0);

      keychooser = new ScrambledZipfianGenerator(recordcount + expectednewkeys);
    } else if (requestdistrib.compareTo("latest") == 0) {
      keychooser = new SkewedLatestGenerator(transactioninsertkeysequence);
    } else if (requestdistrib.equals("hotspot")) {
      final double hotsetfraction = Double.parseDouble(
          p.getProperty(HOTSPOT_DATA_FRACTION, HOTSPOT_DATA_FRACTION_DEFAULT));
      final double hotopnfraction = Double.parseDouble(
          p.getProperty(HOTSPOT_OPN_FRACTION, HOTSPOT_OPN_FRACTION_DEFAULT));
      keychooser = new HotspotIntegerGenerator(0, recordcount - 1,
          hotsetfraction, hotopnfraction);
    } else {
      throw new WorkloadException(
          "Unknown request distribution \"" + requestdistrib + "\"");
    }

    fieldchooser = new UniformIntegerGenerator(0, fieldcount - 1);

    if (scanlengthdistrib.compareTo("uniform") == 0) {
      scanlength = new UniformIntegerGenerator(1, maxscanlength);
    } else if (scanlengthdistrib.compareTo("zipfian") == 0) {
      scanlength = new ZipfianGenerator(1, maxscanlength);
    } else {
      throw new WorkloadException("Distribution \"" + scanlengthdistrib
          + "\" not allowed for scan length");
    }
  }

  /**
   * Results are reported in the first three buckets of the histogram under the
   * label "VERIFY". Bucket 0 means the expected data was returned. Bucket 1
   * means incorrect data was returned. Bucket 2 means null data was returned
   * when some data was expected.
   */
  protected void verifyRow(final String key,
      final HashMap<String, ByteIterator> cells) {
    int matchType = DATA_INT_MATCH;
    if (!cells.isEmpty()) {
      for (final Map.Entry<String, ByteIterator> entry : cells.entrySet()) {
        if (!entry.getValue().toString()
            .equals(buildDeterministicValue(key, entry.getKey()))) {
          matchType = DATA_INT_DEVIATE;
          break;
        }
      }
    } else {
      // This assumes that null data is never valid
      matchType = DATA_INT_UNEXPECTED_NULL;
    }
    Measurements.getMeasurements().measure("VERIFY", matchType);
  }

  int nextKeynum() {
    int keynum;
    if (keychooser instanceof ExponentialGenerator) {
      do {
        keynum = transactioninsertkeysequence.lastInt() - keychooser.nextInt();
      } while (keynum < 0);
    } else {
      do {
        keynum = keychooser.nextInt();
      } while (keynum > transactioninsertkeysequence.lastInt());
    }
    return keynum;
  }

  /**
   * Build a deterministic value given the key information.
   */
  private String buildDeterministicValue(final String key,
      final String fieldkey) {
    final int size = fieldlengthgenerator.nextInt();
    final StringBuilder sb = new StringBuilder(size);
    sb.append(key);
    sb.append(':');
    sb.append(fieldkey);
    while (sb.length() < size) {
      sb.append(':');
      sb.append(sb.toString().hashCode());
    }
    sb.setLength(size);

    return sb.toString();
  }

  /**
   * Builds a value for a randomly chosen field.
   */
  private HashMap<String, ByteIterator> buildSingleValue(final String key) {
    final HashMap<String, ByteIterator> value =
        new HashMap<String, ByteIterator>();

    final String fieldkey =
        fieldnames.get(Integer.parseInt(fieldchooser.nextString()));
    ByteIterator data;
    if (dataintegrity) {
      data = new StringByteIterator(buildDeterministicValue(key, fieldkey));
    } else {
      // fill with random data
      data = new RandomByteIterator(fieldlengthgenerator.nextInt());
    }
    value.put(fieldkey, data);

    return value;
  }

  /**
   * Builds values for all fields.
   */
  private HashMap<String, ByteIterator> buildValues(final String key) {
    final HashMap<String, ByteIterator> values =
        new HashMap<String, ByteIterator>();

    for (final String fieldkey : fieldnames) {
      ByteIterator data;
      if (dataintegrity) {
        data = new StringByteIterator(buildDeterministicValue(key, fieldkey));
      } else {
        // fill with random data
        data = new RandomByteIterator(fieldlengthgenerator.nextInt());
      }
      values.put(fieldkey, data);
    }
    return values;
  }
}
