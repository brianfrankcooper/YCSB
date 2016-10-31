package com.yahoo.ycsb.workloads;

import com.yahoo.ycsb.*;
import com.yahoo.ycsb.generator.*;
import com.yahoo.ycsb.measurements.Measurements;

import java.util.*;

/**
 * Created by rgmacedo on 10/28/16.
 */
public class SafeWorkload extends CoreWorkload {
  public static final String START_KEY_NAME_DEFAULT = "false";
  public static final String START_KEY_NAME = "startkeyname";
  public static String startkeyname;

  int fieldcount;

  private List<String> fieldnames;
  NumberGenerator fieldlengthgenerator;
  boolean readallfields;
  boolean writeallfields;
  private boolean dataintegrity;
  NumberGenerator keysequence;
  DiscreteGenerator operationchooser;
  NumberGenerator keychooser;
  NumberGenerator fieldchooser;
  AcknowledgedCounterGenerator transactioninsertkeysequence;
  NumberGenerator scanlength;
  boolean orderedinserts;
  int recordcount;
  int zeropadding;
  int insertionRetryLimit;
  int insertionRetryInterval;

  private Measurements _measurements = Measurements.getMeasurements();

  /**
   * Initialize the scenario.
   * Called once, in the main client thread, before any operations are started.
   */
  @Override
  public void init(Properties p) throws WorkloadException {
//    Tabela
    table = p.getProperty(TABLENAME_PROPERTY, TABLENAME_PROPERTY_DEFAULT);

//    Start key name
    startkeyname = p.getProperty(START_KEY_NAME, START_KEY_NAME_DEFAULT);

//    get numero de campos
    fieldcount =
      Integer.parseInt(p.getProperty(FIELD_COUNT_PROPERTY, FIELD_COUNT_PROPERTY_DEFAULT));
    fieldnames = new ArrayList<String>();
    for (int i = 0; i < fieldcount; i++) {
      fieldnames.add("field" + i);
    }
//    gerador para o tamanho de cada campo
    fieldlengthgenerator = SafeWorkload.getFieldLengthGenerator(p);

//    numero de records para fazer load inicialmente
    recordcount =
      Integer.parseInt(p.getProperty(Client.RECORD_COUNT_PROPERTY, Client.DEFAULT_RECORD_COUNT));
    if (recordcount == 0) {
      recordcount = Integer.MAX_VALUE;
    }
    String requestdistrib =
      p.getProperty(REQUEST_DISTRIBUTION_PROPERTY, REQUEST_DISTRIBUTION_PROPERTY_DEFAULT);

//    max number of scan's to perform
    int maxscanlength =
      Integer.parseInt(p.getProperty(MAX_SCAN_LENGTH_PROPERTY, MAX_SCAN_LENGTH_PROPERTY_DEFAULT));

//    what distribution should be used for to choose the number of records to scan
    String scanlengthdistrib =
      p.getProperty(SCAN_LENGTH_DISTRIBUTION_PROPERTY, SCAN_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT);

    int insertstart =
      Integer.parseInt(p.getProperty(INSERT_START_PROPERTY, INSERT_START_PROPERTY_DEFAULT));
    int insertcount =
      Integer.parseInt(p.getProperty(INSERT_COUNT_PROPERTY, String.valueOf(recordcount - insertstart)));
    // Confirm valid values for insertstart and insertcount in relation to recordcount
    if (recordcount < (insertstart + insertcount)) {
      System.err.println("Invalid combination of insertstart, insertcount and recordcount.");
      System.err.println("recordcount must be bigger than insertstart + insertcount.");
      System.exit(-1);
    }

//    number of padding to add on keys
    zeropadding =
      Integer.parseInt(p.getProperty(ZERO_PADDING_PROPERTY, ZERO_PADDING_PROPERTY_DEFAULT));

//    ler todos os campos em scan's e get's
    readallfields = Boolean.parseBoolean(
      p.getProperty(READ_ALL_FIELDS_PROPERTY, READ_ALL_FIELDS_PROPERTY_DEFAULT));

//    escrever em todos os campos nos insert's e update's
    writeallfields = Boolean.parseBoolean(
      p.getProperty(WRITE_ALL_FIELDS_PROPERTY, WRITE_ALL_FIELDS_PROPERTY_DEFAULT));

//    check all returned results
    dataintegrity = Boolean.parseBoolean(
      p.getProperty(DATA_INTEGRITY_PROPERTY, DATA_INTEGRITY_PROPERTY_DEFAULT));
    // Confirm that fieldlengthgenerator returns a constant if data
    // integrity check requested.
    if (dataintegrity && !(p.getProperty(
      FIELD_LENGTH_DISTRIBUTION_PROPERTY,
      FIELD_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT)).equals("constant")) {
      System.err.println("Must have constant field size to check data integrity.");
      System.exit(-1);
    }

//    ordem com que os valores são inseridos:
//    default == hashed , hashed order
//    order == ordered by key("ordered")

    if (p.getProperty(INSERT_ORDER_PROPERTY, INSERT_ORDER_PROPERTY_DEFAULT).compareTo("hashed") == 0) {
      orderedinserts = false;
    } else if (requestdistrib.compareTo("exponential") == 0) {
      double percentile = Double.parseDouble(p.getProperty(
        ExponentialGenerator.EXPONENTIAL_PERCENTILE_PROPERTY,
        ExponentialGenerator.EXPONENTIAL_PERCENTILE_DEFAULT));
      double frac = Double.parseDouble(p.getProperty(
        ExponentialGenerator.EXPONENTIAL_FRAC_PROPERTY,
        ExponentialGenerator.EXPONENTIAL_FRAC_DEFAULT));
      keychooser = new ExponentialGenerator(percentile, recordcount * frac);
    } else {
      orderedinserts = true;
    }

    keysequence = new CounterGenerator(insertstart);
    operationchooser = createOperationGenerator(p);

    transactioninsertkeysequence = new AcknowledgedCounterGenerator(recordcount);
    if (requestdistrib.compareTo("uniform") == 0) {
      keychooser = new UniformIntegerGenerator(insertstart, insertstart + insertcount - 1);
    } else if (requestdistrib.compareTo("sequential") == 0) {
      keychooser = new SequentialGenerator(insertstart, insertstart + insertcount - 1);
    }else if (requestdistrib.compareTo("zipfian") == 0) {

      final double insertproportion = Double.parseDouble(
        p.getProperty(INSERT_PROPORTION_PROPERTY, INSERT_PROPORTION_PROPERTY_DEFAULT));
      int opcount = Integer.parseInt(p.getProperty(Client.OPERATION_COUNT_PROPERTY));
      int expectednewkeys = (int) ((opcount) * insertproportion * 2.0); // 2 is fudge factor

      keychooser = new ScrambledZipfianGenerator(insertstart, insertstart + insertcount + expectednewkeys);
    } else if (requestdistrib.compareTo("latest") == 0) {
      keychooser = new SkewedLatestGenerator(transactioninsertkeysequence);
    } else if (requestdistrib.equals("hotspot")) {
      double hotsetfraction =
        Double.parseDouble(p.getProperty(HOTSPOT_DATA_FRACTION, HOTSPOT_DATA_FRACTION_DEFAULT));
      double hotopnfraction =
        Double.parseDouble(p.getProperty(HOTSPOT_OPN_FRACTION, HOTSPOT_OPN_FRACTION_DEFAULT));
      keychooser = new HotspotIntegerGenerator(insertstart, insertstart + insertcount - 1,
        hotsetfraction, hotopnfraction);
    } else {
      throw new WorkloadException("Unknown request distribution \"" + requestdistrib + "\"");
    }

    fieldchooser = new UniformIntegerGenerator(0, fieldcount - 1);

    if (scanlengthdistrib.compareTo("uniform") == 0) {
      scanlength = new UniformIntegerGenerator(1, maxscanlength);
    } else if (scanlengthdistrib.compareTo("zipfian") == 0) {
      scanlength = new ZipfianGenerator(1, maxscanlength);
    } else {
      throw new WorkloadException(
        "Distribution \"" + scanlengthdistrib + "\" not allowed for scan length");
    }

    insertionRetryLimit = Integer.parseInt(p.getProperty(
      INSERTION_RETRY_LIMIT, INSERTION_RETRY_LIMIT_DEFAULT));
    insertionRetryInterval = Integer.parseInt(p.getProperty(
      INSERTION_RETRY_INTERVAL, INSERTION_RETRY_INTERVAL_DEFAULT));
  }

  //TODO remover o "user" e por padding
  public String buildKeyName(long keynum) {
    if (!orderedinserts) {
      keynum = Utils.hash(keynum);
    }
    String value = Long.toString(keynum);
    int fill = zeropadding - value.length();
    String prekey = "user";
    for(int i=0; i<fill; i++) {
      prekey += '0';
    }
    return prekey + value;
  }

  /**
   * Builds a value for a randomly chosen field.
   */
  private HashMap<String, ByteIterator> buildSingleValue(String key) {
    HashMap<String, ByteIterator> value = new HashMap<String, ByteIterator>();

    String fieldkey = fieldnames.get(fieldchooser.nextValue().intValue());
    ByteIterator data;
    if (dataintegrity) {
      data = new StringByteIterator(buildDeterministicValue(key, fieldkey));
    } else {
      // fill with random data
      data = new RandomByteIterator(fieldlengthgenerator.nextValue().longValue());
    }
    value.put(fieldkey, data);

    return value;
  }

  /**
   * Builds values for all fields.
   */
  private HashMap<String, ByteIterator> buildValues(String key) {
    HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();

    for (String fieldkey : fieldnames) {
      ByteIterator data;
      if (dataintegrity) {
        data = new StringByteIterator(buildDeterministicValue(key, fieldkey));
      } else {
        // fill with random data
        data = new RandomByteIterator(fieldlengthgenerator.nextValue().longValue());
      }
      values.put(fieldkey, data);
    }
    return values;
  }

  /**
   * Build a deterministic value given the key information.
   */
  private String buildDeterministicValue(String key, String fieldkey) {
    int size = fieldlengthgenerator.nextValue().intValue();
    StringBuilder sb = new StringBuilder(size);
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
   * Do one insert operation. Because it will be called concurrently from multiple client threads,
   * this function must be thread safe. However, avoid synchronized, or the threads will block waiting
   * for each other, and it will be difficult to reach the target throughput. Ideally, this function would
   * have no side effects other than DB operations.
   */
  @Override
  public boolean doInsert(DB db, Object threadstate) {
    int keynum = keysequence.nextValue().intValue();
    String dbkey = buildKeyName(keynum);
    HashMap<String, ByteIterator> values = buildValues(dbkey);

    Status status;
    int numOfRetries = 0;
    do {
      status = db.insert(table, dbkey, values);
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
   * have no side effects other than DB operations.
   */
  @Override
  public boolean doTransaction(DB db, Object threadstate) {
    switch (operationchooser.nextString()) {
      case "READ":
        doTransactionRead(db);
        break;
      case "UPDATE":
        doTransactionUpdate(db);
        break;
      case "INSERT":
        doTransactionInsert(db);
        break;
      case "SCAN":
        doTransactionScan(db);
        break;
      case "FILTER":
        doTransactionFilter(db);
        break;
      default:
        doTransactionReadModifyWrite(db);
    }

    return true;
  }

  /**
   * Results are reported in the first three buckets of the histogram under
   * the label "VERIFY".
   * Bucket 0 means the expected data was returned.
   * Bucket 1 means incorrect data was returned.
   * Bucket 2 means null data was returned when some data was expected.
   */
  protected void verifyRow(String key, HashMap<String, ByteIterator> cells) {
    Status verifyStatus = Status.OK;
    long startTime = System.nanoTime();
    if (!cells.isEmpty()) {
      for (Map.Entry<String, ByteIterator> entry : cells.entrySet()) {
        if (!entry.getValue().toString().equals(buildDeterministicValue(key, entry.getKey()))) {
          verifyStatus = Status.UNEXPECTED_STATE;
          break;
        }
      }
    } else {
      // This assumes that null data is never valid
      verifyStatus = Status.ERROR;
    }
    long endTime = System.nanoTime();
    _measurements.measure("VERIFY", (int) (endTime - startTime) / 1000);
    _measurements.reportStatus("VERIFY", verifyStatus);
  }

  int nextKeynum() {
    int keynum;
    if (keychooser instanceof ExponentialGenerator) {
      do {
        keynum = transactioninsertkeysequence.lastValue() - keychooser.nextValue().intValue();
      } while (keynum < 0);
    } else {
      do {
        keynum = keychooser.nextValue().intValue();
      } while (keynum > transactioninsertkeysequence.lastValue());
    }
    return keynum;
  }

//  public void doTransactionRead(DB db) {
//    // choose a random key
//    int keynum = nextKeynum();
//
//    String keyname = buildKeyName(keynum);
//
//    HashSet<String> fields = null;
//
//    if (!readallfields) {
//      // read a random field
//      String fieldname = fieldnames.get(fieldchooser.nextValue().intValue());
//
//      fields = new HashSet<String>();
//      fields.add(fieldname);
//    } else if (dataintegrity) {
//      // pass the full field list if dataintegrity is on for verification
//      fields = new HashSet<String>(fieldnames);
//    }
//
//    HashMap<String, ByteIterator> cells = new HashMap<String, ByteIterator>();
//    db.read(table, keyname, fields, cells);
//
//    if (dataintegrity) {
//      verifyRow(keyname, cells);
//    }
//  }
//
//  public void doTransactionReadModifyWrite(DB db) {
//    // choose a random key
//    int keynum = nextKeynum();
//
//    String keyname = buildKeyName(keynum);
//
//    HashSet<String> fields = null;
//
//    if (!readallfields) {
//      // read a random field
//      String fieldname = fieldnames.get(fieldchooser.nextValue().intValue());
//
//      fields = new HashSet<String>();
//      fields.add(fieldname);
//    }
//
//    HashMap<String, ByteIterator> values;
//
//    if (writeallfields) {
//      // new data for all the fields
//      values = buildValues(keyname);
//    } else {
//      // update a random field
//      values = buildSingleValue(keyname);
//    }
//
//    // do the transaction
//
//    HashMap<String, ByteIterator> cells = new HashMap<String, ByteIterator>();
//
//
//    long ist = _measurements.getIntendedtartTimeNs();
//    long st = System.nanoTime();
//    db.read(table, keyname, fields, cells);
//
//    db.update(table, keyname, values);
//
//    long en = System.nanoTime();
//
//    if (dataintegrity) {
//      verifyRow(keyname, cells);
//    }
//
//    _measurements.measure("READ-MODIFY-WRITE", (int) ((en - st) / 1000));
//    _measurements.measureIntended("READ-MODIFY-WRITE", (int) ((en - ist) / 1000));
//  }
//
//  public void doTransactionScan(DB db) {
//    // choose a random key
//    int keynum = nextKeynum();
//
//    String startkeyname = buildKeyName(keynum);
//
//    // choose a random scan length
//    int len = scanlength.nextValue().intValue();
//
//    HashSet<String> fields = null;
//
//    if (!readallfields) {
//      // read a random field
//      String fieldname = fieldnames.get(fieldchooser.nextValue().intValue());
//
//      fields = new HashSet<String>();
//      fields.add(fieldname);
//    }
//
//    db.scan(table, startkeyname, len, fields, new Vector<HashMap<String, ByteIterator>>());
//  }

  public void doTransactionFilter(DB db) {
    String startk = null;

    if(startkeyname.equals("true")) {
//TODO - aqui acho que o nextKeyNum pode levar uma seed
      int keynum = nextKeynum();
      startk = buildKeyName(keynum);
    }
    else
      startk = "false";

    // choose a random scan length
    int len = scanlength.nextValue().intValue();

    //TODO - Para já nao estou a usar a startkey, nem o compare operator, nem o value. ta tudo por default no hbase client
    db.filter(table, startk, len, "user8627391162697748212", "GREATER", new ArrayList<String>());
//    db.filter(table, startkeyname, len, "user8627391162697748212", "GREATER", new Vector<HashMap<String, ByteIterator>>());
  }

//  public void doTransactionUpdate(DB db) {
//    // choose a random key
//    int keynum = nextKeynum();
//
//    String keyname = buildKeyName(keynum);
//
//    HashMap<String, ByteIterator> values;
//
//    if (writeallfields) {
//      // new data for all the fields
//      values = buildValues(keyname);
//    } else {
//      // update a random field
//      values = buildSingleValue(keyname);
//    }
//
//    db.update(table, keyname, values);
//  }
//
//  public void doTransactionInsert(DB db) {
//    // choose the next key
//    int keynum = transactioninsertkeysequence.nextValue();
//
//    try {
//      String dbkey = buildKeyName(keynum);
//
//      HashMap<String, ByteIterator> values = buildValues(dbkey);
//      db.insert(table, dbkey, values);
//    } finally {
//      transactioninsertkeysequence.acknowledge(keynum);
//    }
//  }

  /**
   * Creates a weighted discrete values with database operations for a workload to perform.
   * Weights/proportions are read from the properties list and defaults are used
   * when values are not configured.
   * Current operations are "READ", "UPDATE", "INSERT", "SCAN" and "READMODIFYWRITE".
   * @param p The properties list to pull weights from.
   * @return A generator that can be used to determine the next operation to perform.
   * @throws IllegalArgumentException if the properties object was null.
   */
  public static DiscreteGenerator createOperationGenerator(final Properties p) {
    if (p == null) {
      throw new IllegalArgumentException("Properties object cannot be null");
    }
    final double readproportion = Double.parseDouble(
      p.getProperty(READ_PROPORTION_PROPERTY, READ_PROPORTION_PROPERTY_DEFAULT));
    final double updateproportion = Double.parseDouble(
      p.getProperty(UPDATE_PROPORTION_PROPERTY, UPDATE_PROPORTION_PROPERTY_DEFAULT));
    final double insertproportion = Double.parseDouble(
      p.getProperty(INSERT_PROPORTION_PROPERTY, INSERT_PROPORTION_PROPERTY_DEFAULT));
    final double scanproportion = Double.parseDouble(
      p.getProperty(SCAN_PROPORTION_PROPERTY, SCAN_PROPORTION_PROPERTY_DEFAULT));
    final double filterproportion = Double.parseDouble(
      p.getProperty(FILTER_PROPORTION_PROPERTY, FILTER_PROPORTION_PROPERTY_DEFAULT));
    final double readmodifywriteproportion = Double.parseDouble(p.getProperty(
      READMODIFYWRITE_PROPORTION_PROPERTY, READMODIFYWRITE_PROPORTION_PROPERTY_DEFAULT));

    final DiscreteGenerator operationchooser = new DiscreteGenerator();
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

    if (filterproportion > 0) {
      operationchooser.addValue(filterproportion, "FILTER");
    }

    if (readmodifywriteproportion > 0) {
      operationchooser.addValue(readmodifywriteproportion, "READMODIFYWRITE");
    }
    return operationchooser;
  }
}
