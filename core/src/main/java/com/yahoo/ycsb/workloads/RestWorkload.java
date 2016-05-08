package com.yahoo.ycsb.workloads;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.RandomByteIterator;
import com.yahoo.ycsb.Workload;
import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.generator.ConstantIntegerGenerator;
import com.yahoo.ycsb.generator.DiscreteGenerator;
import com.yahoo.ycsb.generator.ExponentialGenerator;
import com.yahoo.ycsb.generator.HistogramGenerator;
import com.yahoo.ycsb.generator.HotspotIntegerGenerator;
import com.yahoo.ycsb.generator.NumberGenerator;
import com.yahoo.ycsb.generator.UniformIntegerGenerator;
import com.yahoo.ycsb.generator.ZipfianGenerator;

/**
 * Typical RESTFul services benchmarking scenario. Represents a set of client
 * calling REST operations like HTTP DELETE, GET, POST, PUT on a web service.
 * This scenario is completely different from CoreWorkload which is mainly
 * designed for databases benchmarking. However due to the huge overlapping
 * functionality, a major portion of the code in this class is extracted from
 * {@link CoreWorkload} class.
 */
public class RestWorkload extends Workload {

	/**
	 * The name of the property for the proportion of transactions that are
	 * reads.
	 */
	public static final String READ_PROPORTION_PROPERTY = "readproportion";

	/**
	 * The default proportion of transactions that are reads.
	 */
	public static final String READ_PROPORTION_PROPERTY_DEFAULT = "0.95";

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
	 * The name of the property for the proportion of transactions that are
	 * inserts.
	 */
	public static final String INSERT_PROPORTION_PROPERTY = "insertproportion";

	/**
	 * The default proportion of transactions that are inserts.
	 */
	public static final String INSERT_PROPORTION_PROPERTY_DEFAULT = "0.0";

	/**
	 * The name of the property for the proportion of transactions that are
	 * delete.
	 */
	public static final String DELETE_PROPORTION_PROPERTY = "deleteproportion";

	/**
	 * The default proportion of transactions that are delete.
	 */
	public static final String DELETE_PROPORTION_PROPERTY_DEFAULT = "0.00";

	/**
	 * The name of the property for the the distribution of requests across the
	 * keyspace. Options are "uniform", "zipfian" and "latest"
	 */
	public static final String REQUEST_DISTRIBUTION_PROPERTY = "requestdistribution";

	/**
	 * The default distribution of requests across the keyspace
	 */
	public static final String REQUEST_DISTRIBUTION_PROPERTY_DEFAULT = "uniform";

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

	public static final String FIELD_LENGTH_DISTRIBUTION_PROPERTY = "fieldlengthdistribution";

	/**
	 * The default field length distribution.
	 */
	public static final String FIELD_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT = "constant";

	/**
	 * The name of the property for the length of a field in bytes.
	 */
	public static final String FIELD_LENGTH_PROPERTY = "fieldlength";

	/**
	 * The default maximum length of a field in bytes.
	 */
	public static final String FIELD_LENGTH_PROPERTY_DEFAULT = "100";

	/**
	 * The name of a property that specifies the filename containing the field
	 * length histogram (only used if fieldlengthdistribution is "histogram").
	 */
	public static final String FIELD_LENGTH_HISTOGRAM_FILE_PROPERTY = "fieldlengthhistogram";

	/**
	 * The default filename containing a field length histogram.
	 */
	public static final String FIELD_LENGTH_HISTOGRAM_FILE_PROPERTY_DEFAULT = "hist.txt";
	public static final String FIELD_LENGTH_DISTRIBUTION_FILE_PROPERTY = "fieldlengthdistfile";
	public static final String FIELD_LENGTH_DISTRIBUTION_FILE_PROPERTY_DEFAULT = "fieldLengthDistFile.txt";

	/**
	 * Generator object that produces field lengths. The value of this depends
	 * on the properties that start with "FIELD_LENGTH_".
	 */
	private static NumberGenerator fieldLengthGenerator;

	/**
	 * In web services even though the CRUD operations follow the same request
	 * distribution, they have different traces and distribution parameter
	 * values. Hence configuring the parameters of these operations separately
	 * makes the benchmark more flexible and capable of generating better
	 * realistic workloads.
	 */
	// Read related properties.
	private static final String READ_TRACE_FILE = "url.trace.read";
	private static final String READ_TRACE_FILE_DEFAULT = "readtrace.txt";
	private static final String READ_ZIPFIAN_CONSTANT = "readzipfconstant";
	private static final String READ_ZIPFIAN_CONSTANT_DEAFULT = "0.99";
	private static final String READ_RECORD_COUNT_PROPERTY = "readrecordcount";
	// Insert related properties.
	private static final String INSERT_TRACE_FILE = "url.trace.insert";
	private static final String INSERT_TRACE_FILE_DEFAULT = "inserttrace.txt";
	private static final String INSERT_ZIPFIAN_CONSTANT = "insertzipfconstant";
	private static final String INSERT_ZIPFIAN_CONSTANT_DEAFULT = "0.99";
	private static final String INSERT_SIZE_ZIPFIAN_CONSTANT = "insertsizezipfconstant";
	private static final String INSERT_SIZE_ZIPFIAN_CONSTANT_DEAFULT = "0.99";
	private static final String INSERT_RECORD_COUNT_PROPERTY = "insertrecordcount";
	// Delete related properties.
	private static final String DELETE_TRACE_FILE = "url.trace.delete";
	private static final String DELETE_TRACE_FILE_DEFAULT = "deletetrace.txt";
	private static final String DELETE_ZIPFIAN_CONSTANT = "deletezipfconstant";
	private static final String DELETE_ZIPFIAN_CONSTANT_DEAFULT = "0.99";
	private static final String DELETE_RECORD_COUNT_PROPERTY = "deleterecordcount";
	// Delete related properties.
	private static final String UPDATE_TRACE_FILE = "url.trace.update";
	private static final String UPDATE_TRACE_FILE_DEFAULT = "updatetrace.txt";
	private static final String UPDATE_ZIPFIAN_CONSTANT = "updatezipfconstant";
	private static final String UPDATE_ZIPFIAN_CONSTANT_DEAFULT = "0.99";
	private static final String UPDATE_RECORD_COUNT_PROPERTY = "updaterecordcount";

	private static Map<Integer, String> readUrlMap;
	private static Map<Integer, String> insertUrlMap;
	private static Map<Integer, String> deleteUrlMap;
	private static Map<Integer, String> updateUrlMap;
	private static int readRecordCount;
	private static int insertRecordCount;
	private static int deleteRecordCount;
	private static int updateRecordCount;
	private NumberGenerator readKeyChooser;
	private NumberGenerator insertKeyChooser;
	private NumberGenerator deleteKeyChooser;
	private NumberGenerator updateKeyChooser;
	private DiscreteGenerator operationChooser;

	@Override
	public void init(Properties p) throws WorkloadException {

		readRecordCount = Integer.parseInt(p.getProperty(READ_RECORD_COUNT_PROPERTY, String.valueOf(Long.MAX_VALUE)));
		insertRecordCount = Integer
				.parseInt(p.getProperty(INSERT_RECORD_COUNT_PROPERTY, String.valueOf(Long.MAX_VALUE)));
		deleteRecordCount = Integer
				.parseInt(p.getProperty(DELETE_RECORD_COUNT_PROPERTY, String.valueOf(Long.MAX_VALUE)));
		updateRecordCount = Integer
				.parseInt(p.getProperty(UPDATE_RECORD_COUNT_PROPERTY, String.valueOf(Long.MAX_VALUE)));

		if (readUrlMap == null)
			readUrlMap = getTrace(p.getProperty(READ_TRACE_FILE, READ_TRACE_FILE_DEFAULT), readRecordCount);
		if (insertUrlMap == null)
			insertUrlMap = getTrace(p.getProperty(INSERT_TRACE_FILE, INSERT_TRACE_FILE_DEFAULT), insertRecordCount);
		if (deleteUrlMap == null)
			deleteUrlMap = getTrace(p.getProperty(DELETE_TRACE_FILE, DELETE_TRACE_FILE_DEFAULT), deleteRecordCount);
		if (updateUrlMap == null)
			updateUrlMap = getTrace(p.getProperty(UPDATE_TRACE_FILE, UPDATE_TRACE_FILE_DEFAULT), updateRecordCount);

		double readproportion = Double
				.parseDouble(p.getProperty(READ_PROPORTION_PROPERTY, READ_PROPORTION_PROPERTY_DEFAULT));
		double updateproportion = Double
				.parseDouble(p.getProperty(UPDATE_PROPORTION_PROPERTY, UPDATE_PROPORTION_PROPERTY_DEFAULT));
		double insertproportion = Double
				.parseDouble(p.getProperty(INSERT_PROPORTION_PROPERTY, INSERT_PROPORTION_PROPERTY_DEFAULT));
		double deleteproportion = Double
				.parseDouble(p.getProperty(DELETE_PROPORTION_PROPERTY, DELETE_PROPORTION_PROPERTY_DEFAULT));

		operationChooser = new DiscreteGenerator();

		if (readproportion > 0)
			operationChooser.addValue(readproportion, "READ");
		if (updateproportion > 0)
			operationChooser.addValue(updateproportion, "UPDATE");
		if (insertproportion > 0)
			operationChooser.addValue(insertproportion, "INSERT");
		if (deleteproportion > 0)
			operationChooser.addValue(insertproportion, "DELETE");

		// Common distribution for all operations.
		String requestDistrib = p.getProperty(REQUEST_DISTRIBUTION_PROPERTY, REQUEST_DISTRIBUTION_PROPERTY_DEFAULT);

		double readZipfconstant = Double
				.parseDouble(p.getProperty(READ_ZIPFIAN_CONSTANT, READ_ZIPFIAN_CONSTANT_DEAFULT));
		readKeyChooser = getKeyChooser(requestDistrib, readRecordCount, readZipfconstant, p);
		double updateZipfconstant = Double
				.parseDouble(p.getProperty(UPDATE_ZIPFIAN_CONSTANT, UPDATE_ZIPFIAN_CONSTANT_DEAFULT));
		updateKeyChooser = getKeyChooser(requestDistrib, updateRecordCount, updateZipfconstant, p);
		double insertZipfconstant = Double
				.parseDouble(p.getProperty(INSERT_ZIPFIAN_CONSTANT, INSERT_ZIPFIAN_CONSTANT_DEAFULT));
		insertKeyChooser = getKeyChooser(requestDistrib, insertRecordCount, insertZipfconstant, p);
		double deleteZipfconstant = Double
				.parseDouble(p.getProperty(DELETE_ZIPFIAN_CONSTANT, DELETE_ZIPFIAN_CONSTANT_DEAFULT));
		deleteKeyChooser = getKeyChooser(requestDistrib, deleteRecordCount, deleteZipfconstant, p);

		fieldLengthGenerator = getFieldLengthGenerator(p);
	}

	private static NumberGenerator getKeyChooser(String requestDistrib, int recordCount, double zipfContant,
			Properties p) throws WorkloadException {
		NumberGenerator keychooser = null;
		if (requestDistrib.compareTo("exponential") == 0) {
			double percentile = Double.parseDouble(p.getProperty(ExponentialGenerator.EXPONENTIAL_PERCENTILE_PROPERTY,
					ExponentialGenerator.EXPONENTIAL_PERCENTILE_DEFAULT));
			double frac = Double.parseDouble(p.getProperty(ExponentialGenerator.EXPONENTIAL_FRAC_PROPERTY,
					ExponentialGenerator.EXPONENTIAL_FRAC_DEFAULT));
			keychooser = new ExponentialGenerator(percentile, recordCount * frac);
		} else if (requestDistrib.compareTo("uniform") == 0) {
			keychooser = new UniformIntegerGenerator(0, recordCount - 1);
		} else if (requestDistrib.compareTo("zipfian") == 0) {
			keychooser = new ZipfianGenerator(recordCount, zipfContant);
		} else if (requestDistrib.compareTo("latest") == 0) {
			// TODO: To be implemented.
			System.out.println("Latest not supported");
		} else if (requestDistrib.equals("hotspot")) {
			double hotsetfraction = Double
					.parseDouble(p.getProperty(HOTSPOT_DATA_FRACTION, HOTSPOT_DATA_FRACTION_DEFAULT));
			double hotopnfraction = Double
					.parseDouble(p.getProperty(HOTSPOT_OPN_FRACTION, HOTSPOT_OPN_FRACTION_DEFAULT));
			keychooser = new HotspotIntegerGenerator(0, recordCount - 1, hotsetfraction, hotopnfraction);
		} else {
			throw new WorkloadException("Unknown request distribution \"" + requestDistrib + "\"");
		}
		return keychooser;
	}

	private static NumberGenerator getFieldLengthGenerator(Properties p) throws WorkloadException {
		NumberGenerator fieldLengthGenerator;
		String fieldlengthdistribution = p.getProperty(FIELD_LENGTH_DISTRIBUTION_PROPERTY,
				FIELD_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT);
		int fieldlength = Integer.parseInt(p.getProperty(FIELD_LENGTH_PROPERTY, FIELD_LENGTH_PROPERTY_DEFAULT));
		String fieldlengthhistogram = p.getProperty(FIELD_LENGTH_HISTOGRAM_FILE_PROPERTY,
				FIELD_LENGTH_HISTOGRAM_FILE_PROPERTY_DEFAULT);
		if (fieldlengthdistribution.compareTo("constant") == 0) {
			fieldLengthGenerator = new ConstantIntegerGenerator(fieldlength);
		} else if (fieldlengthdistribution.compareTo("uniform") == 0) {
			fieldLengthGenerator = new UniformIntegerGenerator(1, fieldlength);
		} else if (fieldlengthdistribution.compareTo("zipfian") == 0) {
			double insertsizezipfconstant = Double
					.parseDouble(p.getProperty(INSERT_SIZE_ZIPFIAN_CONSTANT, INSERT_SIZE_ZIPFIAN_CONSTANT_DEAFULT));
			fieldLengthGenerator = new ZipfianGenerator(1, fieldlength, insertsizezipfconstant);
		} else if (fieldlengthdistribution.compareTo("histogram") == 0) {
			try {
				fieldLengthGenerator = new HistogramGenerator(fieldlengthhistogram);
			} catch (IOException e) {
				throw new WorkloadException("Couldn't read field length histogram file: " + fieldlengthhistogram, e);
			}
		} else {
			throw new WorkloadException("Unknown field length distribution \"" + fieldlengthdistribution + "\"");
		}
		return fieldLengthGenerator;
	}

	// Reads the trace file and returns a URL map.
	private static synchronized Map<Integer, String> getTrace(String filePath, int recordCount)
			throws WorkloadException {
		Map<Integer, String> urlMap = new ConcurrentHashMap<Integer, String>();
		int count = 0;
		String line;
		try {
			FileReader inputFile = new FileReader(filePath);
			BufferedReader bufferReader = new BufferedReader(inputFile);
			while ((line = bufferReader.readLine()) != null) {
				urlMap.put(count++, line.trim());
				if (count >= recordCount)
					break;
			}
			bufferReader.close();
		} catch (IOException e) {
			throw new WorkloadException(
					"Error while reading the trace. Please make sure the trace file path is correct. "
							+ e.getLocalizedMessage());
		}
		return urlMap;
	}

	/**
	 * Not required for Rest Clients as data population is service specific.
	 */
	@Override
	public boolean doInsert(DB db, Object threadstate) {
		return false;
	}

	/**
	 * Do one transaction operation. Because it will be called concurrently from
	 * multiple client threads, this function must be thread safe. However,
	 * avoid synchronized, or the threads will block waiting for each other, and
	 * it will be difficult to reach the target throughput. Ideally, this
	 * function would have no side effects other than DB operations.
	 */
	@Override
	public boolean doTransaction(DB db, Object threadstate) {
		switch (operationChooser.nextString()) {
		case "UPDATE":
			doTransactionUpdate(db);
			break;
		case "INSERT":
			doTransactionInsert(db);
			break;
		case "DELETE":
			doTransactionDelete(db);
			break;
		default:
			doTransactionRead(db);
		}
		return true;
	}

	private String getNextURL(int opType) {
		if (opType == 1)
			return readUrlMap.get(readKeyChooser.nextValue().intValue());
		else if (opType == 2)
			return insertUrlMap.get(insertKeyChooser.nextValue().intValue());
		else if (opType == 3)
			return deleteUrlMap.get(deleteKeyChooser.nextValue().intValue());
		else
			return updateUrlMap.get(updateKeyChooser.nextValue().intValue());
	}

	private HashMap<String, ByteIterator> getInsertData() {
		HashMap<String, ByteIterator> data = new HashMap<String, ByteIterator>();
		data.put("data", new RandomByteIterator(fieldLengthGenerator.nextValue().intValue()));
		return data;
	}

	public void doTransactionUpdate(DB db) {
		HashMap<String, ByteIterator> result = new HashMap<String, ByteIterator>();
		db.update(null, getNextURL(4), result);
	}

	public void doTransactionInsert(DB db) {
		db.insert(null, getNextURL(2), getInsertData());
	}

	public void doTransactionDelete(DB db) {
		HashMap<String, ByteIterator> result = new HashMap<String, ByteIterator>();
		db.read(null, getNextURL(3), null, result);
	}

	public void doTransactionRead(DB db) {
		HashMap<String, ByteIterator> result = new HashMap<String, ByteIterator>();
		db.read(null, getNextURL(1), null, result);
	}
}
