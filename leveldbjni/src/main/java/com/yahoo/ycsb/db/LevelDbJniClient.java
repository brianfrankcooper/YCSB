/**
 * LevelDB client binding for YCSB.
 */

package com.yahoo.ycsb.db;

import static org.fusesource.leveldbjni.JniDBFactory.factory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.StringByteIterator;

/**
 * LevelDBJni client for YCSB framework.
 */
public class LevelDbJniClient extends DB {

	private static org.iq80.leveldb.DB db = null;

	private static final AtomicInteger initCount = new AtomicInteger(0);

	private synchronized static void getDBInstance() {
		if (db == null) {
			Options options = new Options();
			// options.cacheSize(100 * 1048576); // 100MB cache
			options.createIfMissing(true);
			try {
				db = factory.open(new File("leveldb_database"), options);
			} catch (IOException e) {
				System.out.println("Failed to open database");
				e.printStackTrace();
			}
		}
	}

	private static byte[] mapToBytes(Map<String, String> map)
			throws IOException {
		ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
		ObjectOutputStream out = new ObjectOutputStream(byteOut);
		out.writeObject(map);
		return byteOut.toByteArray();
	}

	private static Map<String, String> bytesToMap(byte[] bytes)
			throws IOException, ClassNotFoundException {
		ByteArrayInputStream byteIn = new ByteArrayInputStream(bytes);
		ObjectInputStream in = new ObjectInputStream(byteIn);
		@SuppressWarnings("unchecked")
		Map<String, String> map = (Map<String, String>) in.readObject();
		return map;
	}

	/**
	 * Initialize any state for this DB. Called once per DB instance; there is
	 * one DB instance per client thread.
	 */
	@Override
	public void init() throws DBException {
		initCount.incrementAndGet();
		getDBInstance();
	}

	/**
	 * Cleanup any state for this DB. Called once per DB instance; there is one
	 * DB instance per client thread.
	 */
	@Override
	public void cleanup() throws DBException {
		if (initCount.decrementAndGet() <= 0) {
			try {
				db.close();
			} catch (IOException e) {
				System.out.println("Failed to close db");
				e.printStackTrace();
			}
		}
	}

	/**
	 * Delete a record from the database.
	 * 
	 * @param table
	 *            The name of the table
	 * @param key
	 *            The record key of the record to delete.
	 * @return Zero on success, a non-zero error code on error. See this class's
	 *         description for a discussion of error codes.
	 */
	@Override
	public int delete(String table, String key) {
		db.delete(key.getBytes());
		return 0;
	}

	/**
	 * Insert a record in the database. Any field/value pairs in the specified
	 * values HashMap will be written into the record with the specified record
	 * key.
	 * 
	 * @param table
	 *            The name of the table
	 * @param key
	 *            The record key of the record to insert.
	 * @param values
	 *            A HashMap of field/value pairs to insert in the record
	 * @return Zero on success, a non-zero error code on error. See this class's
	 *         description for a discussion of error codes.
	 */
	@Override
	public int insert(String table, String key,
			HashMap<String, ByteIterator> values) {
		Map<String, String> stringValues = StringByteIterator
				.getStringMap(values);
		try {
			db.put(key.getBytes(), mapToBytes(stringValues));
		} catch (org.iq80.leveldb.DBException e) {
			System.out.println("Failed to insert " + key);
			e.printStackTrace();
			return 1;
		} catch (IOException e) {
			System.out.println("Failed to insert " + key);
			e.printStackTrace();
			return 1;
		}
		return 0;
	}

	/**
	 * Read a record from the database. Each field/value pair from the result
	 * will be stored in a HashMap.
	 * 
	 * @param table
	 *            The name of the table
	 * @param key
	 *            The record key of the record to read.
	 * @param fields
	 *            The list of fields to read, or null for all of them
	 * @param result
	 *            A HashMap of field/value pairs for the result
	 * @return Zero on success, a non-zero error code on error or "not found".
	 */
	@Override
	public int read(String table, String key, Set<String> fields,
			HashMap<String, ByteIterator> result) {
		byte[] value = db.get(key.getBytes());
		if (value == null) {
			return 1;
		}
		Map<String, String> map;
		try {
			map = bytesToMap(value);
		} catch (IOException e) {
			System.out.println("Failed to read " + key);
			e.printStackTrace();
			return 1;
		} catch (ClassNotFoundException e) {
			System.out.println("Failed to read " + key);
			e.printStackTrace();
			return 1;
		}
		StringByteIterator.putAllAsByteIterators(result, map);
		return 0;
	}

	/**
	 * Update a record in the database. Any field/value pairs in the specified
	 * values HashMap will be written into the record with the specified record
	 * key, overwriting any existing values with the same field name.
	 * 
	 * @param table
	 *            The name of the table
	 * @param key
	 *            The record key of the record to write.
	 * @param values
	 *            A HashMap of field/value pairs to update in the record
	 * @return Zero on success, a non-zero error code on error. See this class's
	 *         description for a discussion of error codes.
	 */
	@Override
	public int update(String table, String key,
			HashMap<String, ByteIterator> values) {
		byte[] existingBytes = db.get(key.getBytes());
		Map<String, String> existingValues;
		if (existingBytes != null) {
			try {
				existingValues = bytesToMap(existingBytes);
			} catch (IOException e) {
				System.out.println("Failed to read for update " + key);
				e.printStackTrace();
				return 1;
			} catch (ClassNotFoundException e) {
				System.out.println("Failed to read for update " + key);
				e.printStackTrace();
				return 1;
			}
		} else {
			existingValues = new HashMap<String, String>();
		}
		Map<String, String> newValues = StringByteIterator.getStringMap(values);
		existingValues.putAll(newValues);
		try {
			db.put(key.getBytes(), mapToBytes(existingValues));
		} catch (org.iq80.leveldb.DBException e) {
			System.out.println("Failed to insert " + key);
			e.printStackTrace();
			return 1;
		} catch (IOException e) {
			System.out.println("Failed to insert " + key);
			e.printStackTrace();
			return 1;
		}
		return 0;
	}

	/**
	 * Perform a range scan for a set of records in the database. Each
	 * field/value pair from the result will be stored in a HashMap.
	 * 
	 * @param table
	 *            The name of the table
	 * @param startkey
	 *            The record key of the first record to read.
	 * @param recordcount
	 *            The number of records to read
	 * @param fields
	 *            The list of fields to read, or null for all of them
	 * @param result
	 *            A Vector of HashMaps, where each HashMap is a set field/value
	 *            pairs for one record
	 * @return Zero on success, a non-zero error code on error. See this class's
	 *         description for a discussion of error codes.
	 */
	@Override
	public int scan(String table, String startkey, int recordcount,
			Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
		DBIterator iterator = db.iterator();
		int count = 0;
		try {
			iterator.seek(startkey.getBytes());
			while (iterator.hasNext() && count < recordcount) {
				String key = iterator.peekNext().getKey().toString();
				if (fields == null || fields.contains(key)) {
					HashMap<String, String> value;
					value = (HashMap<String, String>) bytesToMap(iterator
							.peekNext().getValue());
					HashMap<String, ByteIterator> byteValues = new HashMap<String, ByteIterator>();
					StringByteIterator.putAllAsByteIterators(byteValues, value);
					result.addElement(byteValues);
				}
				iterator.next();
				count += 1;
			}
		} catch (IOException e) {
			System.out.println("Failed to scan");
			e.printStackTrace();
			return 1;
		} catch (ClassNotFoundException e) {
			System.out.println("Failed to scan");
			e.printStackTrace();
			return 1;
		} finally {
			try {
				iterator.close();
			} catch (IOException e) {
				System.out.println("Failed to close iterator");
				e.printStackTrace();
			}
		}
		return 0;
	}
}
