package com.yahoo.ycsb.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;

public class MySQLClient extends DB {

	public static final String URL_KEY = "mysql:url";
	public static final String USER_KEY = "mysql:user";
	public static final String PASSWORD_KEY = "mysql:password";

	public static final String RECORD_TABLE = "record";
	public static final String VALUE_TABLE = "fieldvalue";

	public static final String RECORD_ID = "recordid";
	public static final String KEY_COL = "recordkey";
	public static final String FIELD_COL = "recordfield";
	public static final String VALUE_COL = "fvalue";

	private static final String READ_STMT_FMT =
		"SELECT r.%s, v.%s " +
		"FROM %s r JOIN %s v ON r.%s = v.%s " +
		"WHERE r.%s = ? AND r.%s in (%s)";

	private static final String READ_SUB_STMT_FMT =
		"SELECT r.%s " +
		"FROM %s r " + 
		"WHERE r.%s = ?";

	private static final String SCAN_STMT_FMT =
		"SELECT r.%s, r.%s, v.%s " +
		"FROM %s r JOIN %s v ON r.%s = v.%s " +
		"WHERE r.%s in (%s)" +
		"ORDER BY r.%s ASC" +
		"LIMIT ? OFFSET ?";

	private static final String UPDATE_STMT_FMT =
		"UPDATE %s r JOIN %s v ON r.%s = v.%s " +
		"SET v.%s = ? " +
		"WHERE r.%s = ? AND r.%s = ?";

	private static final String INSERT_STMT = "CALL kvinsert(?, ?, ?)";

	private static final String DELETE_STMT_FMT =
		"DELETE r, v FROM %s r, %s v " +
		"WHERE r.%s = v.%s " +
		"  AND r.%s = ?";

	private Connection con;
	private boolean debug = false;

	/**
	 * Initialize any state for this DB.
	 * Called once per DB instance; there is one DB instance per client thread.
	 */
	@Override
	public void init() throws DBException {
		if (getProperties().getProperty("verbose", "false").equalsIgnoreCase("true")) debug = true;
		if (getProperties().getProperty("debug", "false").equalsIgnoreCase("true")) debug = true;

		String url = getProperties().getProperty(URL_KEY, "");
		String usr = getProperties().getProperty(USER_KEY, "");
		String password = getProperties().getProperty(PASSWORD_KEY, "");

		if(url.equals("")) throw new DBException("Must specify a MySQL connection url");
		if(usr.equals("")) throw new DBException("Must specify a MySQL connection username");

		try {
			if(debug) System.out.println("Opening connection...");
			con = DriverManager.getConnection(url, usr, password);
		} catch(SQLException e) {
			throw new DBException("Failed to establish connection", e);
		}
	}

    /**
	 * Cleanup any state for this DB.
	 * Called once per DB instance; there is one DB instance per client thread.
	 */
	@Override
	public void cleanup() throws DBException {
		try {
			if(debug) System.out.println("Closing connection...");
			con.close();
		} catch (SQLException e) {
			throw new DBException("cleanup failed", e);
		}
	}

	/**
	 * Read a record from the database. Each field/value pair from the result will be stored in a HashMap.
	 *
	 * @param table The name of the table
	 * @param key The record key of the record to read.
	 * @param fields The list of fields to read, or null for all of them
	 * @param result A HashMap of field/value pairs for the result
	 * @return Zero on success, a non-zero error code on error
	 */
	@Override
	public int read(String table, String key, Set<String> fields, HashMap<String, String> result) {
		StringBuilder sb = new StringBuilder();

		if (null == fields) {
			sb.append(String.format(READ_SUB_STMT_FMT, FIELD_COL, RECORD_TABLE, KEY_COL));
		} else {
			Iterator<String> it = fields.iterator();
			while (it.hasNext()) {
				sb.append("'").append(it.next()).append("'");
				if (it.hasNext()) sb.append(",");
			}
		}

		String readSql = String.format(READ_STMT_FMT, FIELD_COL, VALUE_COL, RECORD_TABLE, VALUE_TABLE, RECORD_ID, RECORD_ID, KEY_COL, FIELD_COL, sb.toString());

		try {
			if(debug) {
				System.out.println("Executing query: " + readSql);
				System.out.println("  with key: " + key);
			}

			PreparedStatement s = con.prepareStatement(readSql);
			s.setString(1, key);
			if (null == fields)
				s.setString(2, key);
			ResultSet r = s.executeQuery();

			while (r.next())
				result.put(r.getString(1), r.getString(2));
			if(debug) System.out.println("-- " + result.size() + " results retrieved.");
		} catch (SQLException e) {
			return -1;
		}
		return 0;
	}

	/**
	 * Perform a range scan for a set of records in the database. Each field/value pair from the result will be stored in a HashMap.
	 *
	 * @param table The name of the table
	 * @param startkey The record key of the first record to read.
	 * @param recordcount The number of records to read
	 * @param fields The list of fields to read, or null for all of them
	 * @param result A Vector of HashMaps, where each HashMap is a set field/value pairs for one record
	 * @return Zero on success, a non-zero error code on error
	 */
	@Override
	public int scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, String>> result) {
		StringBuilder sb = new StringBuilder();
		for(String f : fields)
			sb.append(f).append(",");

		String scanSql = String.format(SCAN_STMT_FMT, KEY_COL, FIELD_COL, VALUE_COL, RECORD_TABLE, VALUE_TABLE, RECORD_ID, RECORD_ID, FIELD_COL, sb.toString(), KEY_COL);

		try {
			if(debug) {
				System.out.println("Executing query: " + scanSql);
				System.out.println("  with key:     " + startkey);
				System.out.println("  with records: " + recordcount);
			}

			PreparedStatement s = con.prepareStatement(scanSql);
			s.setString(1, startkey);
			s.setInt(2, recordcount);
			ResultSet r = s.executeQuery();

			int ptr = -1;
			String lastKey = "";
			while (r.next()) {
				if (!r.getString(1).equalsIgnoreCase(lastKey)) {
					lastKey = r.getString(1);
					ptr++;
				}
				result.get(ptr).put(r.getString(2), r.getString(3));
			}
			if(debug) System.out.println("-- " + result.size() + " results retrieved.");
		} catch (SQLException e) {
			return -1;
		}
		return 0;
	}

	/**
	 * Update a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
	 * record key, overwriting any existing values with the same field name.
	 *
	 * @param table The name of the table
	 * @param key The record key of the record to write.
	 * @param values A HashMap of field/value pairs to update in the record
	 * @return Zero on success, a non-zero error code on error
	 */
	@Override
	public int update(String table, String key, HashMap<String, String> values) {
		String updateSql = String.format(UPDATE_STMT_FMT, RECORD_TABLE, VALUE_TABLE, RECORD_ID, RECORD_ID, VALUE_COL, KEY_COL, FIELD_COL);

		for (Map.Entry<String, String> e : values.entrySet()) {
			try {
				if(debug) {
					System.out.println("Executing query: " + updateSql);
					System.out.println("  with key:   " + key);
					System.out.println("  with field: " + e.getKey());
					System.out.println("  with value: " + e.getValue());
				}

				PreparedStatement s = con.prepareStatement(updateSql);
				s.setString(1, e.getValue());
				s.setString(2, key);
				s.setString(3, e.getKey());
				s.executeUpdate();
			} catch (SQLException ex) {
				return -1;
			}
		}
		return 0;
	}

	/**
	 * Insert a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
	 * record key.
	 *
	 * @param table The name of the table
	 * @param key The record key of the record to insert.
	 * @param values A HashMap of field/value pairs to insert in the record
	 * @return Zero on success, a non-zero error code on error
	 */
	@Override
	public int insert(String table, String key, HashMap<String, String> values) {
		for (Map.Entry<String, String> e : values.entrySet()) {
			try {
				if(debug) {
					System.out.println("Executing query: " + INSERT_STMT);
					System.out.println("  with key:   " + key);
					System.out.println("  with field: " + e.getKey());
					System.out.println("  with value: " + e.getValue());
				}

				PreparedStatement s = con.prepareCall(INSERT_STMT);
				s.setString(1, key);
				s.setString(2, e.getKey());
				s.setString(3, e.getValue());
				s.execute();
			} catch (SQLException ex) {
				return -1;
			}
		}
		return 0;
	}

	/**
	 * Delete a record from the database. 
	 *
	 * @param table The name of the table
	 * @param key The record key of the record to delete.
	 * @return Zero on success, a non-zero error code on error
	 */
	@Override
	public int delete(String table, String key) {

		String deleteSql = String.format(DELETE_STMT_FMT, RECORD_TABLE, VALUE_TABLE, RECORD_ID, RECORD_ID, KEY_COL);
		try {
			if(debug) {
				System.out.println("Executing query: " + deleteSql);
				System.out.println("  with key:   " + key);
			}

			PreparedStatement s = con.prepareStatement(deleteSql);
			s.setString(1, key);
			s.execute();
		} catch (SQLException ex) {
			return -1;
		}
		return 0;
	}
}
