package com.yahoo.ycsb.db;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.CleanUp;
import org.apache.hadoop.io.Text;
import org.apache.zookeeper.KeeperException;

import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;

public class AccumuloClient extends DB {
	// Error code constants.
	public static final int Ok = 0;
	public static final int ServerError = -1;
	public static final int HttpError = -2;
	public static final int NoMatchingRecord = -3;

	private ZooKeeperInstance _inst;
	private Connector _connector;
	private String _table = "";
	private BatchWriter _bw = null;
	private Text _colFam = new Text("");
	private Scanner _singleScanner = null;    // A scanner for reads/deletes.
	private Scanner _scanScanner = null;      // A scanner for use by scan()

	private static final String PC_PRODUCER = "producer";
	private static final String PC_CONSUMER = "consumer";
	private String _PC_FLAG = "";
	private ZKProducerConsumer.Queue q = null;
	private static Hashtable<String,Long> hmKeyReads = null;
	private static Hashtable<String,Integer> hmKeyNumReads = null;
	private Random r = null;


	@Override
	public void init() throws DBException {
		_colFam = new Text(getProperties().getProperty("accumulo.columnFamily"));

		_inst = new ZooKeeperInstance(getProperties().getProperty("accumulo.instanceName"),
				getProperties().getProperty("accumulo.zooKeepers"));
		try {
			String principal = getProperties().getProperty("accumulo.username");
			AuthenticationToken token = new PasswordToken(getProperties().getProperty("accumulo.password"));
			_connector = _inst.getConnector(principal, token);
		} catch (AccumuloException e) {
			throw new DBException(e);
		} catch (AccumuloSecurityException e) {
			throw new DBException(e);
		}

		_PC_FLAG = getProperties().getProperty("accumulo.PC_FLAG","none");
		if (_PC_FLAG.equals(PC_PRODUCER) || _PC_FLAG.equals(PC_CONSUMER)) {
			System.out.println("*** YCSB Client is "+_PC_FLAG);
			String address = getProperties().getProperty("accumulo.PC_SERVER");
			String root = getProperties().getProperty("accumulo.PC_ROOT_IN_ZK");
			System.out.println("*** PC_INFO(server:"+address+";root="+root+")");
			q = new ZKProducerConsumer.Queue(address, root);
			r = new Random();
		}

		if (_PC_FLAG.equals(PC_CONSUMER)) {
			hmKeyReads = new Hashtable<String,Long>();
			hmKeyNumReads = new Hashtable<String,Integer>();
			keyNotification(null);
		}
	}


	@Override
	public void cleanup() throws DBException
	{
		try {
			if (_bw != null) {
				_bw.close();
			}
		} catch (MutationsRejectedException e) {
			throw new DBException(e);
		}
		CleanUp.shutdownNow();
	}

	/**
	 * Commonly repeated functionality: Before doing any operation, make sure
	 * we're working on the correct table. If not, open the correct one.
	 * 
	 * @param table
	 */
	public void checkTable(String table) throws TableNotFoundException {
		if (!_table.equals(table)) {
			getTable(table);
		}
	}

	/**
	 * Called when the user specifies a table that isn't the same as the
	 * existing table. Connect to it and if necessary, close our current
	 * connection.
	 * 
	 * @param table
	 */
	public void getTable(String table) throws TableNotFoundException {
		if (_bw != null) { // Close the existing writer if necessary.
			try {
				_bw.close();
			} catch (MutationsRejectedException e) {
				// Couldn't spit out the mutations we wanted.
				// Ignore this for now.
			}
		}

		BatchWriterConfig bwc = new BatchWriterConfig();
		bwc.setMaxLatency(Long.parseLong(getProperties().getProperty("accumulo.batchWriterMaxLatency", "30000")), TimeUnit.MILLISECONDS);
		bwc.setMaxMemory(Long.parseLong(getProperties().getProperty("accumulo.batchWriterSize", "100000")));
		bwc.setMaxWriteThreads(Integer.parseInt(getProperties().getProperty("accumulo.batchWriterThreads", "1")));

		_bw = _connector.createBatchWriter(table, bwc);

		// Create our scanners
		_singleScanner = _connector.createScanner(table, Authorizations.EMPTY);
		_scanScanner = _connector.createScanner(table, Authorizations.EMPTY);

		_table = table; // Store the name of the table we have open.
	}

	/**
	 * Gets a scanner from Accumulo over one row
	 * 
	 * @param row the row to scan
	 * @param fields the set of columns to scan
	 * @return an Accumulo {@link Scanner} bound to the given row and columns
	 */
	private Scanner getRow(Text row, Set<String> fields) 
	{
		_singleScanner.clearColumns();
		_singleScanner.setRange(new Range(row));
		if (fields != null) {
			for(String field:fields)
			{
				_singleScanner.fetchColumn(_colFam, new Text(field));
			}
		}
		return _singleScanner;
	}

	@Override
	public int readOne(String table, String key, String field, Map<String,ByteIterator> result) {
		return read(table, key, result);
	}

	@Override
	public int readAll(String table, String key, Map<String,ByteIterator> result) {
		return read(table, key, result);
	}

	public int read(String table, String key, Map<String, ByteIterator> result) {

		try {
			checkTable(table);
		} catch (TableNotFoundException e) {
			System.err.println("Error trying to connect to Accumulo table." + e);
			return ServerError;
		}

		try {
			// Pick out the results we care about.
			for (Entry<Key, Value> entry : getRow(new Text(key), null)) {
			    Value v = entry.getValue();
			    byte[] buf = v.get();
			    result.put(entry.getKey().getColumnQualifier().toString(), 
				       new ByteArrayByteIterator(buf));
			}
		} catch (Exception e) {
			System.err.println("Error trying to reading Accumulo table" + key + e);
			return ServerError;
		}
		return Ok;

	}

	@Override
	public int scanOne(String table, String startkey, int recordcount, String field, List<Map<String, ByteIterator>> result) {

		_scanScanner.clearColumns();
		_scanScanner.setRange(new Range(new Text(startkey), null));
		_scanScanner.fetchColumn(_colFam, new Text(field));

		return scan(table, startkey, recordcount, result);
	}

	@Override
	public int scanAll(String table, String startkey, int recordcount, List<Map<String, ByteIterator>> result) {

		_scanScanner.clearColumns();
		_scanScanner.setRange(new Range(new Text(startkey), null));

		return scan(table, startkey, recordcount, result);
	}

	public int scan(String table, String startkey, int recordcount,
			List<Map<String, ByteIterator>> result) {
		try {
			checkTable(table);
		} catch (TableNotFoundException e) {
			System.err.println("Error trying to connect to Accumulo table." + e);
			return ServerError;
		}

		// There doesn't appear to be a way to create a range for a given
		// LENGTH. Just start and end keys. So we'll do this the hard way for now: 
		// Just make the end 'infinity' and only read as much as we need. 
		_scanScanner.clearColumns();
		_scanScanner.setRange(new Range(new Text(startkey), null));

		// Batch size is how many key/values to try to get per call. Here, I'm
		// guessing that the number of keys in a row is equal to the number of fields
		// we're interested in.
		// We try to fetch one or more using either ScanOne or scanAll respectively so
		// as to tell when we've run out of fields.


		String rowKey = "";
		HashMap<String, ByteIterator> currentHM = null;
		int count = 0;

		// Begin the iteration.
		for (Entry<Key, Value> entry : _scanScanner) {
			// Check for a new row.
			if (!rowKey.equals(entry.getKey().getRow().toString())) {
				if (count++ == recordcount) { // Done reading the last row.
					break;
				}
				rowKey = entry.getKey().getRow().toString();
                currentHM = new HashMap<String, ByteIterator>();

				result.add(currentHM);
			}
			// Now add the key to the hashmap.
			Value v = entry.getValue();
		    byte[] buf = v.get();
			currentHM.put(entry.getKey().getColumnQualifier().toString(), new ByteArrayByteIterator(buf));
		}

		return Ok;

	}

	@Override
	public int updateOne(String table, String key, String field, ByteIterator value) {

		Mutation mutInsert = new Mutation(new Text(key));
		mutInsert.put(_colFam, new Text(field), System.currentTimeMillis(),
				new Value(value.toArray()));

		return update(table, key, mutInsert);
	}

	@Override
	public int updateAll(String table, String key, Map<String,ByteIterator> values) {

		Mutation mutInsert = new Mutation(new Text(key));
		for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
			mutInsert.put(_colFam, new Text(entry.getKey()), System
					.currentTimeMillis(),
					new Value(entry.getValue().toArray()));
		}

		return update(table, key, mutInsert);
	}

	public int update(String table, String key, /*Map<String, ByteIterator> values*/Mutation mutInsert) {
		try {
			checkTable(table);
		} catch (TableNotFoundException e) {
			System.err.println("Error trying to connect to Accumulo table." + e);
			return ServerError;
		}

		try {
			_bw.addMutation(mutInsert);
			// Distributed YCSB co-ordination: YCSB on a client produces the key to 
			// be stored in the shared queue in ZooKeeper.
			if (_PC_FLAG.equals(PC_PRODUCER)) {
				if (r.nextFloat() < 0.01)
					keyNotification(key);
			}
		} catch (MutationsRejectedException e) {
			System.err.println("Error performing update.");
			e.printStackTrace();
			return ServerError;
		}


		return Ok;
	}

	@Override
	public int insert(String table, String key, Map<String, ByteIterator> values) {
		return updateAll(table, key, values);
	}

	@Override
	public int delete(String table, String key) {
		try {
			checkTable(table);
		} catch (TableNotFoundException e) {
			System.err.println("Error trying to connect to Accumulo table." + e);
			return ServerError;
		}

		try {
			deleteRow(new Text(key));
		} catch (RuntimeException e) {
			System.err.println("Error performing delete.");
			e.printStackTrace();
			return ServerError;
		}

		return Ok;
	}

	// These functions are adapted from RowOperations.java:
	private void deleteRow(Text row) {
		deleteRow(getRow(row, null));
	}


	/**
	 * Deletes a row, given a Scanner of JUST that row
	 * 
	 */
	private void deleteRow(Scanner scanner) {
		Mutation deleter = null;
		// iterate through the keys
		for (Entry<Key,Value> entry : scanner) {
			// create a mutation for the row
			if (deleter == null)
				deleter = new Mutation(entry.getKey().getRow());
			// the remove function adds the key with the delete flag set to true
			deleter.putDelete(entry.getKey().getColumnFamily(), entry.getKey().getColumnQualifier());
		}
		try {
			_bw.addMutation(deleter);
		} catch (MutationsRejectedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private void keyNotification(String key) {

		if (_PC_FLAG.equals(PC_PRODUCER)) {
			try {
				q.produce(key);
			} catch (KeeperException e) {

			} catch (InterruptedException e) {

			}
		} else {
			//XXX: do something better to keep the loop going (while??)
			for (int i = 0; i < 10000000; i++) {
				try {
					String strKey = q.consume();
			
					if ((hmKeyReads.containsKey(strKey) == false) && 
							(hmKeyNumReads.containsKey(strKey) == false)) { 
						hmKeyReads.put(strKey, new Long(System.currentTimeMillis()));
						hmKeyNumReads.put(strKey, new Integer(1));
					}

					//YCSB Consumer will read the key that was fetched from the 
					//queue in ZooKeeper.
					//(current way is kind of ugly but works, i think)
					//TODO : Get table name from configuration or argument
					String table = "usertable";
					HashSet<String> fields = new HashSet<String>();
					for (int j=0; j<9; j++)
						fields.add("field"+j);
					HashMap<String,ByteIterator> result = new HashMap<String,ByteIterator>();

					int retval = read(table, strKey, result);
					//If the results are empty, the key is enqueued in Zookeeper
					//and tried again, until the results are found.
					if (result.size() == 0) { 
						q.produce(strKey);
						int count = ((Integer)hmKeyNumReads.get(strKey)).intValue(); 
						hmKeyNumReads.put(strKey, new Integer(count+1));
					}
					else {
						if (((Integer)hmKeyNumReads.get(strKey)).intValue() > 1) { 
							long currTime = System.currentTimeMillis();
							long writeTime = ((Long)hmKeyReads.get(strKey)).longValue();
							System.out.println("Key="+strKey+
									//";StartSearch="+writeTime+
									//";EndSearch="+currTime+
									";TimeLag="+(currTime-writeTime));
						}
					}

				} catch (KeeperException e) {

				} catch (InterruptedException e) {

				}
			}
		}

	}

	public int presplit(String table, String[] keys)
	{
		TreeSet<Text> splits = new TreeSet<Text>();
		for (int i = 0;i < keys.length; i ++)
		{
			splits.add(new Text(keys[i]));
		}
		try {
			_connector.tableOperations().addSplits(table, splits);
		} catch (TableNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (AccumuloException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (AccumuloSecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		return Ok;
	}

}
