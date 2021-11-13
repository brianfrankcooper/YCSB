package site.ycsb.workloads;

import com.github.javafaker.Faker;
import org.javatuples.Pair;
import site.ycsb.*;
import site.ycsb.generator.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

/**
 *
 */
public class CommerceWorkload extends CoreWorkload {
  /**
   * The name of the database table to run queries against.
   */
  public static final String TABLENAME_PROPERTY = "table";

  /**
   * The default name of the database table to run queries against.
   */
  public static final String TABLENAME_PROPERTY_DEFAULT = "products";

  /**
   * The name of the property for the min search length (number of records).
   */
  public static final String MIN_SEARCH_LENGTH_PROPERTY = "minsearchlength";

  public static final String INDEXED_FIELDS_SEARCH_PROPERTY = "indexedfields";
  public static final String INDEXED_FIELDS_SEARCH_PROPERTY_DEFAULT =
      "productName";

  public static final String SEARCH_WORDS_DICT_PROPERTY = "dictfile";
  public static final String SEARCH_WORDS_DICT_DEFAULT =
      "uci_online_retail.txt";

  public static final String NON_INDEXED_FIELDS_SEARCH_PROPERTY = "nonindexedfields";
  public static final String NON_INDEXED_FIELDS_SEARCH_PROPERTY_DEFAULT =
      "nonindexedfields=productScore,code,image,price,currencyCode,stockCount,creator,shipsFrom";


  public static final String SEARCH_FIELDS_PROPORTION_PROPERTY = "searchfieldsproportion";
  public static final String SEARCH_FIELDS_PROPORTION_PROPERTY_DEFAULT = "0.70,0.10,0.05,0.05,0.05,0.05";

  public static final String SEARCH_FIELDS_PROPERTY = "searchfields";
  public static final String SEARCH_FIELDS_PROPERTY_DEFAULT = "department,brand,productName,color,inStock";


  /**
   * The default min search length.
   */
  public static final String MIN_SEARCH_LENGTH_PROPERTY_DEFAULT = "5";
  /**
   * The name of the property for the max search length (number of records).
   */
  public static final String MAX_SEARCH_LENGTH_PROPERTY = "maxsearchlength";
  /**
   * The default max search length.
   */
  public static final String MAX_SEARCH_LENGTH_PROPERTY_DEFAULT = "50";
  /**
   * The name of the property for the search length distribution. Options are "uniform" and "zipfian"
   * (favoring short pages)
   */
  public static final String SEARCH_LENGTH_DISTRIBUTION_PROPERTY = "searchlengthdistribution";

  public static final String SEARCH_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT = "zipfian";
  private static final String SEARCH_PROPORTION_PROPERTY = "searchproportion";
  private static final String SEARCH_PROPORTION_PROPERTY_DEFAULT = "0.6";
  protected NumberGenerator searchlength;
  private String[] indexedFields;
  private ArrayList<Double> indexedFieldsProportionPDF;
  private ArrayList<String> searchDict;
  private int ingestDictPos;
  private Faker faker;
  private Random rand;

  public static String buildKeyName(long keynum, int zeropadding, boolean orderedinserts) {
//    if (!orderedinserts) {
//      keynum = Utils.hash(keynum);
//    }
    String value = Long.toString(keynum);
    int fill = zeropadding - value.length();
    String prekey = "product";
    for (int i = 0; i < fill; i++) {
      prekey += '0';
    }
    return prekey + value;
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
    final double searchproportion = Double.parseDouble(
        p.getProperty(SEARCH_PROPORTION_PROPERTY, SEARCH_PROPORTION_PROPERTY_DEFAULT));

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

    if (searchproportion > 0) {
      operationchooser.addValue(searchproportion, "SEARCH");
    }

    return operationchooser;
  }


  /**
   * Initialize the scenario.
   * Called once, in the main client thread, before any operations are started.
   */
  @Override
  public void init(Properties p) throws WorkloadException {
    ingestDictPos = 0;
    rand = new Random(12345);
    faker = new Faker(new Locale("en"), rand);
    table = p.getProperty(TABLENAME_PROPERTY, TABLENAME_PROPERTY_DEFAULT);

    recordcount =
        Long.parseLong(p.getProperty(Client.RECORD_COUNT_PROPERTY, Client.DEFAULT_RECORD_COUNT));
    if (recordcount == 0) {
      recordcount = Integer.MAX_VALUE;
    }
    String requestdistrib =
        p.getProperty(REQUEST_DISTRIBUTION_PROPERTY, REQUEST_DISTRIBUTION_PROPERTY_DEFAULT);

    long insertstart =
        Long.parseLong(p.getProperty(INSERT_START_PROPERTY, INSERT_START_PROPERTY_DEFAULT));
    long insertcount =
        Integer.parseInt(p.getProperty(INSERT_COUNT_PROPERTY, String.valueOf(recordcount - insertstart)));
    // Confirm valid values for insertstart and insertcount in relation to recordcount
    if (recordcount < (insertstart + insertcount)) {
      System.err.println("Invalid combination of insertstart, insertcount and recordcount.");
      System.err.println("recordcount must be bigger than insertstart + insertcount.");
      System.exit(-1);
    }
    zeropadding =
        Integer.parseInt(p.getProperty(ZERO_PADDING_PROPERTY, ZERO_PADDING_PROPERTY_DEFAULT));

    orderedinserts = p.getProperty(INSERT_ORDER_PROPERTY, INSERT_ORDER_PROPERTY_DEFAULT).compareTo("hashed") != 0;

    keysequence = new CounterGenerator(insertstart);
    operationchooser = createOperationGenerator(p);

    transactioninsertkeysequence = new AcknowledgedCounterGenerator(recordcount);
    if (requestdistrib.compareTo("uniform") == 0) {
      keychooser = new UniformLongGenerator(insertstart, insertstart + insertcount - 1);
    } else if (requestdistrib.compareTo("exponential") == 0) {
      double percentile = Double.parseDouble(p.getProperty(
          ExponentialGenerator.EXPONENTIAL_PERCENTILE_PROPERTY,
          ExponentialGenerator.EXPONENTIAL_PERCENTILE_DEFAULT));
      double frac = Double.parseDouble(p.getProperty(
          ExponentialGenerator.EXPONENTIAL_FRAC_PROPERTY,
          ExponentialGenerator.EXPONENTIAL_FRAC_DEFAULT));
      keychooser = new ExponentialGenerator(percentile, recordcount * frac);
    } else if (requestdistrib.compareTo("sequential") == 0) {
      keychooser = new SequentialGenerator(insertstart, insertstart + insertcount - 1);
    } else if (requestdistrib.compareTo("zipfian") == 0) {
      // it does this by generating a random "next key" in part by taking the modulus over the
      // number of keys.
      // If the number of keys changes, this would shift the modulus, and we don't want that to
      // change which keys are popular so we'll actually construct the scrambled zipfian generator
      // with a keyspace that is larger than exists at the beginning of the test. that is, we'll predict
      // the number of inserts, and tell the scrambled zipfian generator the number of existing keys
      // plus the number of predicted keys as the total keyspace. then, if the generator picks a key
      // that hasn't been inserted yet, will just ignore it and pick another key. this way, the size of
      // the keyspace doesn't change from the perspective of the scrambled zipfian generator
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

    insertionRetryLimit = Integer.parseInt(p.getProperty(
        INSERTION_RETRY_LIMIT, INSERTION_RETRY_LIMIT_DEFAULT));
    insertionRetryInterval = Integer.parseInt(p.getProperty(
        INSERTION_RETRY_INTERVAL, INSERTION_RETRY_INTERVAL_DEFAULT));

    int minsearchlength =
        Integer.parseInt(p.getProperty(MIN_SEARCH_LENGTH_PROPERTY, MIN_SEARCH_LENGTH_PROPERTY_DEFAULT));
    int maxsearchlength =
        Integer.parseInt(p.getProperty(MAX_SEARCH_LENGTH_PROPERTY, MAX_SEARCH_LENGTH_PROPERTY_DEFAULT));
    String searchlengthdistrib =
        p.getProperty(SEARCH_LENGTH_DISTRIBUTION_PROPERTY, SEARCH_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT);

    if (searchlengthdistrib.compareTo("uniform") == 0) {
      searchlength = new UniformLongGenerator(minsearchlength, maxsearchlength);
    } else if (searchlengthdistrib.compareTo("zipfian") == 0) {
      searchlength = new ZipfianGenerator(minsearchlength, maxsearchlength);
    } else {
      throw new WorkloadException(
          "Distribution \"" + searchlengthdistrib + "\" not allowed for search length");
    }

    indexedFields = p.getProperty(SEARCH_FIELDS_PROPERTY,
        SEARCH_FIELDS_PROPERTY_DEFAULT).split(",");
    String[] indexedFieldsProportionStr = p.getProperty(SEARCH_FIELDS_PROPORTION_PROPERTY,
        SEARCH_FIELDS_PROPORTION_PROPERTY_DEFAULT).split(",");
    indexedFieldsProportionPDF = new ArrayList<Double>();
    double currentPDF = 0.0;
    for (String indexedFieldProportion : indexedFieldsProportionStr) {
      currentPDF += Double.parseDouble(indexedFieldProportion);
      indexedFieldsProportionPDF.add(currentPDF);
    }

    BufferedReader reader;
    String filename = p.getProperty(SEARCH_WORDS_DICT_PROPERTY,
        SEARCH_WORDS_DICT_DEFAULT);
    System.err.println("Loading search dictionary from: " + filename);
    searchDict = new ArrayList<String>();
    try {

      reader = new BufferedReader(new FileReader(
          filename));
      String line = reader.readLine();
      while (line != null) {
        line = reader.readLine();
        if (line != null){
          line = line.replaceAll("[^\\p{L} \\p{Nd}]+", "");
          searchDict.add(line);
          if(searchDict.size()%100000==0){
            System.err.println("Read " + searchDict.size());
          }
        }
      }
      reader.close();
      System.err.println("using a dictionary of " + searchDict.size() + " to generate productName and search terms");
    } catch (IOException e) {
      e.printStackTrace();
    }

  }

  /**
   * Do one insert operation. Because it will be called concurrently from multiple client threads, this
   * function must be thread safe. However, avoid synchronized, or the threads will block waiting for each
   * other, and it will be difficult to reach the target throughput. Ideally, this function would have no side
   * effects other than DB operations and mutations on threadstate. Mutations to threadstate do not need to be
   * synchronized, since each thread has its own threadstate instance.
   *
   * @param db
   * @param threadstate
   */
  @Override
  public boolean doInsert(DB db, Object threadstate) {
    int keynum = keysequence.nextValue().intValue();
    String dbkey = buildKeyName(keynum, zeropadding, orderedinserts);
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

  public boolean doTransaction(DB db, Object threadstate) {
    String operation = operationchooser.nextString();
    if (operation == null) {
      return false;
    }
    switch (operation) {
    case "READ":
      doTransactionRead(db);
      break;
    case "UPDATE":
      doTransactionUpdate(db);
      break;
    case "INSERT":
      doTransactionInsert(db);
      break;
    case "SEARCH":
      doTransactionSearch(db);
      break;
    default:
      return false;
    }

    return true;
  }


  long nextKeynum() {
    long keynum;
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

  public void doTransactionRead(DB db) {
    // choose a random key
    long keynum = nextKeynum();
    String keyname = buildKeyName(keynum, zeropadding, orderedinserts);
    HashSet<String> fields = null;

    HashMap<String, ByteIterator> cells = new HashMap<String, ByteIterator>();
    db.read(table, keyname, fields, cells);
  }

  public void doTransactionUpdate(DB db) {
    // choose a random key
    long keynum = nextKeynum();
    String keyname = buildKeyName(keynum, zeropadding, orderedinserts);

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

  public void doTransactionInsert(DB db) {
    // choose the next key
    long keynum = transactioninsertkeysequence.nextValue();

    try {
      String keyname = buildKeyName(keynum, zeropadding, orderedinserts);

      HashMap<String, ByteIterator> values = buildValues(keyname);
      db.insert(table, keyname, values);
    } finally {
      transactioninsertkeysequence.acknowledge(keynum);
    }
  }


  public void doTransactionSearch(DB db) {
    // choose a random search length
    int len = searchlength.nextValue().intValue();
    int replystartpos = 0;

    HashSet<String> fields = null;
    String fieldName = randomIndexedFieldName();
    String textquerytosearch = "*";
    textquerytosearch = getRandomFieldValue(fieldName, textquerytosearch);
    boolean onlyinsale = ThreadLocalRandom.current()
        .nextBoolean();
    Pair<String, String> queryPair = Pair.with(fieldName, textquerytosearch);
    Pair<Integer, Integer> pagePair = Pair.with(replystartpos, len);
    Vector<HashMap<String, ByteIterator>> results = new Vector<HashMap<String, ByteIterator>>();
    long st = System.nanoTime();
    db.search(table, queryPair, onlyinsale, pagePair, fields, results);
    long en = System.nanoTime();
//    results.size()
    measurements.measure("SEARCH", (int) ((en - st) / 1000));
  }

  private String randomIndexedFieldName() {
    double rnd = ThreadLocalRandom.current()
        .nextDouble();
    String fieldName = indexedFields[0];
    double fieldPDF = indexedFieldsProportionPDF.get(0);
    for (int pos = 1; rnd < fieldPDF && pos < indexedFieldsProportionPDF.size(); pos++) {
      fieldPDF = indexedFieldsProportionPDF.get(pos);
      fieldName = indexedFields[pos];
    }
    return fieldName;
  }

  private String getRandomFieldValue(String fieldName, String textquerytosearch) {
    switch (fieldName) {
    case "brand":
      textquerytosearch = faker.company().name().replaceAll("[^a-zA-Z0-9]", " ");
      break;
    case "color":
      textquerytosearch = faker.color().name().replaceAll("[^a-zA-Z0-9]", " ");
      break;
    case "department":
      textquerytosearch = faker.commerce().department().replaceAll("[^a-zA-Z0-9]", " ");
      break;
    case "productName":
      textquerytosearch = searchDict.get(rand.nextInt(searchDict.size()));
      while (textquerytosearch.split(" ").length < 2){
        textquerytosearch = searchDict.get(rand.nextInt(searchDict.size()));
      }
      break;
    case "productDescription":
      textquerytosearch = faker.company().catchPhrase().split(" ")[0];
      break;
    default:
      break;
    }
    return textquerytosearch;
  }

  /**
   * Builds values for all fields.
   */
  protected HashMap<String, ByteIterator> buildValues(String key) {
    if (ingestDictPos>=searchDict.size()){
      ingestDictPos=0;
    }
    HashMap<String, ByteIterator> values = new HashMap<>();
    String productName = searchDict.get(ingestDictPos);
    values.put("productName", new StringByteIterator(productName));
    values.put("productScore", new StringByteIterator(String.valueOf(1.0 - ThreadLocalRandom.current()
        .nextDouble())));
    values.put("code", new StringByteIterator(faker.code().ean13()));
    values.put("productDescription", new StringByteIterator(faker.company().catchPhrase()));
    values.put("department", new StringByteIterator(faker.commerce().department()
        .replaceAll("[^a-zA-Z0-9]", " ")));
    // 0 for out-of-stock
    // 1 for in-stock
    values.put("inStock", new StringByteIterator(String.valueOf(ThreadLocalRandom.current()
        .nextInt(2))));
    // 0 for standardPrice
    // 1 for inSale
    values.put("inSale", new StringByteIterator(String.valueOf(ThreadLocalRandom.current()
        .nextInt(2))));
    values.put("color", new StringByteIterator(faker.commerce().color().
        replaceAll("[^a-zA-Z0-9]", " ")));
    values.put("image", new StringByteIterator(faker.company().logo()));
    values.put("material", new StringByteIterator(faker.commerce().material()
        .replaceAll("[^a-zA-Z0-9]", " ")));
    values.put("price", new StringByteIterator(faker.commerce().price()));
    values.put("currencyCode", new StringByteIterator(faker.currency().code()));
    values.put("brand", new StringByteIterator(faker.company().name()
        .replaceAll("[^a-zA-Z0-9]", " ")));
    values.put("stockCount", new StringByteIterator(String.valueOf(ThreadLocalRandom.current()
        .nextInt(501))));
    values.put("creator", new StringByteIterator(faker.artist().name()
        .replaceAll("[^a-zA-Z0-9]", " ")));
    values.put("shipsFrom", new StringByteIterator(faker.country().countryCode3()));
    ingestDictPos++;
    return values;
  }

  /**
   * Builds a value for a randomly chosen indexed field.
   */
  private HashMap<String, ByteIterator> buildSingleValue(String key) {
    HashMap<String, ByteIterator> value = new HashMap<>();
    String fieldName = randomIndexedFieldName();
    String fieldValue = getRandomFieldValue(fieldName, "");
    value.put(fieldName, new StringByteIterator(fieldValue));
    return value;
  }

}
