package com.yahoo.ycsb.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.StringByteIterator;

import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR;

/**
 * YCSB implementation for Apache Phoenix http://phoenix.apache.org/
 * 
 * Use Phoenix create table utility to create table before running benchmark. 
 * See @PhoenixCreateTable for usage instructions
 * 
 * @author mujtaba
 */
public class PhoenixDbClient extends DB implements PhoenixConstants {
	private Connection connection = null;
	private PreparedStatement upsertStatement = null;
	private PreparedStatement scanStatement = null;
	private PreparedStatement readStatement = null;
	private PreparedStatement deleteStatement = null;
	private int upsertBatchSize = 0;
	private int upsertCount = 1;
	
	/**
	 *  Optional parameters: 1. zookeeper port. default value localhost
		                     2. upsert batch size. default is 0 
	 */
	@Override
	public void init() throws DBException {
		Properties props = getProperties();
		String zookeeper = props.getProperty("zookeeper", "localhost");
		upsertBatchSize = Integer.parseInt(props
				.getProperty("upsertbatch", "0"));

		try {
			connection = DriverManager.getConnection(JDBC_PROTOCOL
					+ JDBC_PROTOCOL_SEPARATOR + zookeeper, new Properties());

			// Auto-commit is turned on by default unless batch size is set
			connection.setAutoCommit(upsertBatchSize == 0 ? true : false);
			System.out.println("Phoenix connection initialized.");
		} catch (Exception e) {
			System.err.println("Could not initialize Phoenix connection.");
			e.printStackTrace();
		}
	}

	/**
	 * Clean up
	 */
	@Override
	public void cleanup() throws DBException {
		try {
			if (upsertStatement != null)
				upsertStatement.close();
			if (scanStatement != null)
				scanStatement.close();
			if (readStatement != null)
				readStatement.close();
			if (deleteStatement != null)
				deleteStatement.close();
			connection.close();
		} catch (Exception e1) {
			System.err.println("Could not cleanly close Phoenix: "
					+ e1.toString());
			e1.printStackTrace();
			return;
		}
	}

	/**
	 * Phoenix insert
	 */
	@Override
	public int insert(String table, String key,
			HashMap<String, ByteIterator> values) {
		int index = 1;
		int ret = -1;
		try {
			if (upsertStatement == null)
				upsertStatement = getUpsertStatement(table, values.size());
			upsertStatement.setString(index++, key);
			for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
				String field = entry.getValue().toString();
				upsertStatement.setString(index++, field);
			}
			ret = upsertStatement.executeUpdate() == 1 ? SUCCESS_CODE
					: FAILURE_CODE;
			upsertStatement.clearParameters();

			// batch commit if batch size is set
			if (upsertBatchSize != 0) {
				if (upsertCount++ % upsertBatchSize == 0) {
					connection.commit();
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return ret;
	}

	/**
	 * Phoenix update
	 */
	@Override
	public int update(String table, String key,
			HashMap<String, ByteIterator> values) {
		return insert(table, key, values);
	}

	/**
	 * Phoenix read
	 */
	@Override
	public int read(String table, String key, Set<String> fields,
			HashMap<String, ByteIterator> result) {
		try {
			if (readStatement == null)
				readStatement = getReadStatement(table);
			readStatement.setString(1, key);
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return getReadResultSet(readStatement, fields, result);
	}

	/**
	 * Phoenix scan
	 */
	@Override
	public int scan(String table, String startkey, int recordcount,
			Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
		try {
			if (scanStatement == null)
				scanStatement = getScanStatement(table);
			scanStatement.setString(1, startkey);
			scanStatement.setInt(2, recordcount);
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return getScanResultSet(scanStatement, recordcount, fields, result);
	}

	/**
	 * 
	 * @param statement
	 * @param fields
	 * @param result
	 * @return
	 */
	private int getReadResultSet(PreparedStatement statement,
			Set<String> fields, HashMap<String, ByteIterator> result) {
		try {
			ResultSet resultSet = statement.executeQuery();
			statement.clearParameters();
			if (resultSet.next()) {
				if (result != null && fields != null) {
					for (String field : fields) {
						String value = resultSet.getString(field);
						result.put(field, new StringByteIterator(value));
					}
				}
			}
			resultSet.close();
			return 0;

		} catch (Exception e) {
			e.printStackTrace();
			return -1;
		}
	}

	/**
	 * 
	 * @param statement
	 * @param recordcount
	 * @param fields
	 * @param result
	 * @return
	 */
	private int getScanResultSet(PreparedStatement statement, int recordcount,
			Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
		try {
			ResultSet resultSet = statement.executeQuery();
			statement.clearParameters();

			for (int i = 0; i < recordcount && resultSet.next(); i++) {
				if (result != null && fields != null) {
					HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
					for (String field : fields) {
						String value = resultSet.getString(field);
						values.put(field, new StringByteIterator(value));
					}
					result.add(values);
				}
			}
			resultSet.close();
			return 0;

		} catch (Exception e) {
			e.printStackTrace();
			return -1;
		}
	}

	/**
	 * Phoenix delete
	 */
	@Override
	public int delete(String table, String key) {
		int ret = -1;
		try {
			if (deleteStatement == null)
				deleteStatement = getDeleteStatement(table);
			deleteStatement.setString(1, key);
			ret = deleteStatement.executeUpdate() == 1 ? SUCCESS_CODE
					: FAILURE_CODE;
			deleteStatement.clearParameters();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return ret;
	}

	/**
	 * Read statement
	 * 
	 * @param tableName
	 * @return
	 * @throws SQLException
	 */
	private PreparedStatement getReadStatement(String tableName)
			throws SQLException {
		StringBuilder sql = new StringBuilder("SELECT * FROM ");
		sql.append(tableName);
		sql.append(" WHERE ");
		sql.append(PRIMARY_KEY);
		sql.append(" = ");
		sql.append("?");
		return connection.prepareStatement(sql.toString());
	}

	/**
	 * Scan statement
	 * 
	 * @param tableName
	 * @return
	 * @throws SQLException
	 */
	private PreparedStatement getScanStatement(String tableName)
			throws SQLException {
		StringBuilder sql = new StringBuilder("SELECT * FROM ");
		sql.append(tableName);
		sql.append(" WHERE ");
		sql.append(PRIMARY_KEY);
		sql.append(" >= ");
		sql.append("? LIMIT ?");
		return connection.prepareStatement(sql.toString());
	}

	/**
	 * Upsert statement
	 * 
	 * @param tableName
	 * @param fieldcount
	 * @return
	 * @throws SQLException
	 */
	private PreparedStatement getUpsertStatement(String tableName,
			int fieldcount) throws SQLException {
		StringBuilder sql = new StringBuilder("UPSERT INTO ");
		sql.append(tableName);
		sql.append(" VALUES(?");
		for (int i = 0; i < fieldcount; i++) {
			sql.append(",?");
		}
		sql.append(")");

		return connection.prepareStatement(sql.toString());
	}

	/**
	 * Delete statement
	 * 
	 * @param tableName
	 * @return
	 * @throws SQLException
	 */
	private PreparedStatement getDeleteStatement(String tableName)
			throws SQLException {
		StringBuilder sql = new StringBuilder("DELETE INTO ");
		sql.append(tableName);
		sql.append(" WHERE ");
		sql.append(PRIMARY_KEY);
		sql.append(" = ");
		sql.append("?");
		return connection.prepareStatement(sql.toString());
	}
}
