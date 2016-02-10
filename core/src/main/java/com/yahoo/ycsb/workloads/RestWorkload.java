package com.yahoo.ycsb.workloads;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.Client;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.Workload;
import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.generator.AcknowledgedCounterGenerator;
import com.yahoo.ycsb.generator.DiscreteGenerator;
import com.yahoo.ycsb.generator.ExponentialGenerator;
import com.yahoo.ycsb.generator.HotspotIntegerGenerator;
import com.yahoo.ycsb.generator.NumberGenerator;
import com.yahoo.ycsb.generator.SkewedLatestGenerator;
import com.yahoo.ycsb.generator.UniformIntegerGenerator;
import com.yahoo.ycsb.generator.ZipfianGenerator;

/**
 * Typical RESTFul services benchmarking scenario. Represents a set of client
 * calling REST operations like HTTP DELETE, GET, POST, PUT on a web service.
 * This scenario is completely different from CoreWorkload which is mainly
 * designed for databases benchmarking. However due to the huge overlapping
 * functionality, a major portion of the code in this class is extracted from
 * {@link CoreWorkload} class.
 * 
 * @author shivam.maharshi
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

	/**
	 * Generator object that produces field lengths. The value of this depends
	 * on the properties that start with "FIELD_LENGTH_".
	 */
	private NumberGenerator fieldlengthgenerator;

	public static final String TRACE_FILE = "urltrace";

	public static final String TRACE_FILE_DEFAULT = "trace.txt";

	public static final String ZIPFIAN_CONSTANT = "zipfconstant";
	public static final String ZIPFIAN_CONSTANT_DEAFULT = "0.99";

	private static Map<Long, String> urlMap;
	private static int recordcount;
	private NumberGenerator keychooser;
	private AcknowledgedCounterGenerator transactioninsertkeysequence;
	private DiscreteGenerator operationchooser;

	@Override
	public void init(Properties p) throws WorkloadException {

		recordcount = Integer.parseInt(p.getProperty(Client.RECORD_COUNT_PROPERTY, Client.DEFAULT_RECORD_COUNT));

		if (urlMap == null) {
			String traceFile = p.getProperty(TRACE_FILE, TRACE_FILE_DEFAULT);
			urlMap = getTrace(traceFile, recordcount);
		}

		operationchooser = new DiscreteGenerator();

		double readproportion = Double
				.parseDouble(p.getProperty(READ_PROPORTION_PROPERTY, READ_PROPORTION_PROPERTY_DEFAULT));
		double updateproportion = Double
				.parseDouble(p.getProperty(UPDATE_PROPORTION_PROPERTY, UPDATE_PROPORTION_PROPERTY_DEFAULT));
		double insertproportion = Double
				.parseDouble(p.getProperty(INSERT_PROPORTION_PROPERTY, INSERT_PROPORTION_PROPERTY_DEFAULT));
		double deleteproportion = Double
				.parseDouble(p.getProperty(DELETE_PROPORTION_PROPERTY, DELETE_PROPORTION_PROPERTY_DEFAULT));

		if (readproportion > 0) {
			operationchooser.addValue(readproportion, "READ");
		}

		if (updateproportion > 0) {
			operationchooser.addValue(updateproportion, "UPDATE");
		}

		if (insertproportion > 0) {
			operationchooser.addValue(insertproportion, "INSERT");
		}

		if (deleteproportion > 0) {
			operationchooser.addValue(insertproportion, "DELETE");
		}

		String requestdistrib = p.getProperty(REQUEST_DISTRIBUTION_PROPERTY, REQUEST_DISTRIBUTION_PROPERTY_DEFAULT);
		transactioninsertkeysequence = new AcknowledgedCounterGenerator(recordcount);
		if (requestdistrib.compareTo("exponential") == 0) {
			double percentile = Double.parseDouble(p.getProperty(ExponentialGenerator.EXPONENTIAL_PERCENTILE_PROPERTY,
					ExponentialGenerator.EXPONENTIAL_PERCENTILE_DEFAULT));
			double frac = Double.parseDouble(p.getProperty(ExponentialGenerator.EXPONENTIAL_FRAC_PROPERTY,
					ExponentialGenerator.EXPONENTIAL_FRAC_DEFAULT));
			keychooser = new ExponentialGenerator(percentile, recordcount * frac);
		} else if (requestdistrib.compareTo("uniform") == 0) {
			keychooser = new UniformIntegerGenerator(0, recordcount - 1);
		} else if (requestdistrib.compareTo("zipfian") == 0) {
			double readzipfconstant = Double.parseDouble(p.getProperty(ZIPFIAN_CONSTANT, ZIPFIAN_CONSTANT_DEAFULT));
			keychooser = new ZipfianGenerator(recordcount, readzipfconstant);
		} else if (requestdistrib.compareTo("latest") == 0) {
			keychooser = new SkewedLatestGenerator(transactioninsertkeysequence);
		} else if (requestdistrib.equals("hotspot")) {
			double hotsetfraction = Double
					.parseDouble(p.getProperty(HOTSPOT_DATA_FRACTION, HOTSPOT_DATA_FRACTION_DEFAULT));
			double hotopnfraction = Double
					.parseDouble(p.getProperty(HOTSPOT_OPN_FRACTION, HOTSPOT_OPN_FRACTION_DEFAULT));
			keychooser = new HotspotIntegerGenerator(0, recordcount - 1, hotsetfraction, hotopnfraction);
		}
	};

	// Reads the trace file and returns a URL map.
	private static synchronized Map<Long, String> getTrace(String filePath, int recordCount) throws WorkloadException {
		if (urlMap != null)
			return urlMap;
		Map<Long, String> urlMap = new ConcurrentHashMap<Long, String>();
		long count = 0;
		String line;
		try {
			FileReader inputFile = new FileReader(filePath);
			BufferedReader bufferReader = new BufferedReader(inputFile);
			while ((line = bufferReader.readLine()) != null) {
				urlMap.put(count++, line.trim());
				if (count == recordCount)
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

	@Override
	public boolean doInsert(DB db, Object threadstate) {
		// Not required for Rest Clients.
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
		switch (operationchooser.nextString()) {
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

	public String getNextURL() {
		return urlMap.get(keychooser.nextValue().longValue());
	}

	private HashMap<String, ByteIterator> getInsertData() {
		// TODO:
		return null;
	}

	public void doTransactionUpdate(DB db) {
		HashMap<String, ByteIterator> result = new HashMap<String, ByteIterator>();
		db.update(null, getNextURL(), result);
	}

	public void doTransactionInsert(DB db) {
		db.insert(null, getNextURL(), getInsertData());
	}

	public void doTransactionDelete(DB db) {
		HashMap<String, ByteIterator> result = new HashMap<String, ByteIterator>();
		db.read(null, getNextURL(), null, result);
	}

	public void doTransactionRead(DB db) {
		HashMap<String, ByteIterator> result = new HashMap<String, ByteIterator>();
		db.read(null, getNextURL(), null, result);
	}
}
