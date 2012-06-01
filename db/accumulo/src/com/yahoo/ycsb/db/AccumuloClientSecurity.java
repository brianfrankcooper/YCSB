package com.yahoo.ycsb.db;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.security.TablePermission;

import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.security.AccessControlList;
import com.yahoo.ycsb.security.CellAccessControlList;
import com.yahoo.ycsb.security.ColumnAccessControlList;
import com.yahoo.ycsb.security.Credential;
import com.yahoo.ycsb.security.Permission;
import com.yahoo.ycsb.security.TableAccessControlList;

public class AccumuloClientSecurity extends DB {

	// Error code constants.
	public static final int Ok = 0;
	public static final int ServerError = -1;
	public static final int HttpError = -2;
	public static final int NoMatchingRecord = -3;

	private Connector _connector;
	private String _table = "";
	private BatchWriter _bw = null;
	private Text _colFam = new Text("");
	private Scanner _singleScanner = null; // A scanner for reads/deletes.
	private Scanner _scanScanner = null; // A scanner for use by scan()
	private Authorizations _authorizations = Constants.NO_AUTHS;
	private Map<String, ColumnVisibility> _column_acl = new HashMap<String, ColumnVisibility>();
	private Map<Map.Entry<String, String>, ColumnVisibility> _cell_acl = new HashMap<Map.Entry<String, String>, ColumnVisibility>();

	@Override
	public void setCredential(Credential credential) {
		_authorizations = new Authorizations(credential.getName());

		try {
			_connector.securityOperations().changeUserAuthorizations(getProperties().getProperty("cloudbase.username"), _authorizations);
		} catch (AccumuloException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (AccumuloSecurityException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}


		if(!_table.equals("")) {
			try {
				getTable(_table);
			} catch (TableNotFoundException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public void setSchemaAccessControl(List<AccessControlList> acl) {
		try {
			_column_acl = new HashMap<String, ColumnVisibility>();
			for (AccessControlList entry : acl) {
				if (entry instanceof TableAccessControlList) {
					for (String name : entry.getEntries()) {
						if((entry.getPermission() == Permission.READ) || (entry.getPermission() == Permission.READWRITE))
							_connector.securityOperations().grantTablePermission(
									name, entry.getTablename(),
									TablePermission.READ);
						if((entry.getPermission() == Permission.WRITE) || (entry.getPermission() == Permission.READWRITE))
							_connector.securityOperations().grantTablePermission(
									name, entry.getTablename(),
									TablePermission.WRITE);
					}
				} else if (entry instanceof ColumnAccessControlList) {
					String list = "";
					for (String name : entry.getEntries()) {
						if(!list.equals(""))
							list = list + "|";
						list = list + name;
					}
					_column_acl.put(((ColumnAccessControlList) entry).getColumnname(), new ColumnVisibility(list));					
				} else {
					throw new UnsupportedOperationException(entry.getClass()
							+ " is not supported by setSchemaAccessControl()");
				}
			}
		} catch (AccumuloException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (AccumuloSecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@Override
	public void setOperationAccessControl(List<AccessControlList> acl) {
		_cell_acl = new HashMap<Map.Entry<String, String>, ColumnVisibility>();
		for (AccessControlList entry : acl) {
			if (entry instanceof CellAccessControlList) {
				String list = "";
				for (String name : entry.getEntries()) {
					if(!list.equals(""))
						list = list + "|";
					list = list + name;
				}
				_cell_acl.put(new AbstractMap.SimpleImmutableEntry<String, String>(((CellAccessControlList) entry).getRowname(), ((CellAccessControlList) entry).getColumnname()), new ColumnVisibility(list));				
			} else {
				throw new UnsupportedOperationException(entry.getClass()
						+ " is not supported by setSchemaAccessControl()");
			}
		}
	}

	public void init() throws DBException {
		_colFam = new Text(getProperties().getProperty("cloudbase.columnFamily"));
		// First the setup work
		Instance inst = new ZooKeeperInstance(getProperties().getProperty("accumulo.instanceName"),
				getProperties().getProperty("accumulo.zooKeepers"));
		try {
			_connector = inst.getConnector(getProperties().getProperty("accumulo.username"), getProperties().getProperty("accumulo.password"));
		} catch (AccumuloException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (AccumuloSecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	/**
	 * Cleanup any state for this DB.
	 * Called once per DB instance; there is one DB instance per client thread.
	 */
	public void cleanup() throws DBException
	{
		try {
			if (_bw != null) {
				_bw.close();
			}
		} catch (MutationsRejectedException e) {
			throw new DBException(e);
		}
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

		try {
			_bw = _connector.createBatchWriter(table, 100000L, 30000L, 1);
			// Create our scann = nullers
			_singleScanner = _connector.createScanner(table, _authorizations);
			_scanScanner = _connector.createScanner(table, _authorizations);		
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		_table = table; // Store the name of the table we have open.
	}

	public int delete(String table, String key) {
		try {
			checkTable(table);
		} catch (TableNotFoundException e) {
			System.err.println("Error trying to connect to cloudbase table." + e);
			return ServerError;
		}

		try {
			deleteRow(table, new Text(key));
		} catch (Exception e) {
			System.err.println("Error while deleting row " + key + e);
			return ServerError;
		}

		return 0;
	}

	/**
	 * Insert a new record. This is just an update.
	 */
	public int insert(String table, String key, HashMap<String, String> values) {
		return update(table, key, values);
	}

	public int read(String table, String key, Set<String> fields,
			HashMap<String, String> result) {
		try {
			checkTable(table);
		} catch (TableNotFoundException e) {
			System.err.println("Error trying to connect to cloudbase table." + e);
			return ServerError;
		}

		try {
			// Pick out the results we care about.
			for (Entry<Key, Value> entry : getRow(table, new Text(key), null)) {
				result.put(entry.getKey().getColumnQualifier().toString(), 
						entry.getValue().toString());
			}
		} catch (Exception e) {
			System.err.println("Error trying to reading cloudbase table" + key + e);
			return ServerError;
		}
		return 0;
	}

	public int scan(String table, String startkey, int recordcount,
			Set<String> fields, Vector<HashMap<String, String>> result) {
		try {
			checkTable(table);
		} catch (TableNotFoundException e) {
			System.err.println("Error trying to connect to cloudbase table." + e);
			return ServerError;
		}


		// There doesn't appear to be a way to create a range for a given
		// LENGTH. Just start and
		// end keys. So we'll do this the hard way for now: Just make the end
		// 'infinity' and only
		// read as much as we need. We should look into setBatchSize(), though.
		_scanScanner.clearColumns();
		_scanScanner.setRange(new Range(new Text(startkey), null));


		if (fields != null) {
			for(String field:fields)
			{
				_scanScanner.fetchColumn(_colFam, new Text(field));
			}
		} else {
			// If no fields are provided, we assume one column/row.
			//_scanScanner.setBatchSize(recordcount + 1);
		}

		String rowKey = "";
		HashMap<String, String> currentHM = null;
		int count = 0;

		// Begin the iteration.
		for (Entry<Key, Value> entry : _scanScanner) {
			// Check for a new row.
			if (!rowKey.equals(entry.getKey().getRow().toString())) {
				if (count++ == recordcount) { // Done reading the last row.
					break;
				}
				rowKey = entry.getKey().getRow().toString();
				if (fields != null) {
					// Initial Capacity for all keys.
					currentHM = new HashMap<String, String>(fields.size()); 
				}
				else
				{
					// An empty result map.
					currentHM = new HashMap<String, String>();
				}
				result.add(currentHM);
			}
			// Now add the key to the hashmap.
			currentHM.put(entry.getKey().getColumnQualifier().toString(), entry.getValue().toString());
		}

		return 0;
	}

	public int update(String table, String key, HashMap<String, String> values) {
		//		Thread.dumpStack();
		try {
			checkTable(table);
		} catch (TableNotFoundException e) {
			System.err.println("Error trying to connect to cloudbase table." + e);
			return ServerError;
		}

		Mutation mutInsert = new Mutation(new Text(key));
		for (Map.Entry<String, String> entry : values.entrySet()) {
			ColumnVisibility vis = _cell_acl.get(new AbstractMap.SimpleImmutableEntry<String, String>(key, entry.getKey()));

			if(vis == null) {
				vis = _column_acl.get(entry.getKey());
			}

			if (vis != null) {
				mutInsert.put(_colFam, new Text(entry.getKey()), vis, System
						.currentTimeMillis(), new Value(entry.getValue()
								.getBytes()));
			} else {
				mutInsert.put(_colFam, new Text(entry.getKey()), System
						.currentTimeMillis(), new Value(entry.getValue()
								.getBytes()));
			}
		}

		try {
			_bw.addMutation(mutInsert);
		} catch (Exception e) {
			System.err.println("Error trying to update " + key + e);
			return ServerError;
		}
		return 0;
	}

	// These functions are adapted from RowOperations.java:
	private void deleteRow(String table, Text row) {
		deleteRow(table, getRow(table, row, null));
	}

	/**
	 * Deletes a row, given a Scanner of JUST that row
	 * 
	 */
	private void deleteRow(String table, Scanner scanner) {
		Mutation deleter = null;
		// iterate through the keys and create a deleter mutataion to remove them all.
		for (Entry<Key, Value> entry : scanner) {
			if (deleter == null)
				deleter = new Mutation(entry.getKey().getRow());
			// the remove function adds the key with the delete flag set to true
			deleter.putDelete(entry.getKey().getColumnFamily(), entry.getKey()
					.getColumnQualifier());
		}
		try {
			_bw.addMutation(deleter);
		} catch (MutationsRejectedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * Gets a scanner from CB over one row
	 * 
	 * @param row
	 * @return
	 * @throws TableNotFoundException
	 * @throws CBSecurityException
	 * @throws CBException
	 * @throws IOException
	 */
	private Scanner getRow(String table, Text row, Set<String> fields) {
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

	public static void main(String[] args) {
		if (args.length<3)
		{
			System.out.println("I am just testing!");
			System.exit(0);
		}
	}
}
