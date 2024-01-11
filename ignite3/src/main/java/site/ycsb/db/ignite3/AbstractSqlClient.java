package site.ycsb.db.ignite3;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import site.ycsb.ByteIterator;
import site.ycsb.DBException;

abstract class AbstractSqlClient extends IgniteAbstractClient {
  /** SQL string of prepared statement for reading values. */
  protected static String readPreparedStatementString;

  /** SQL string of prepared statement for inserting values. */
  protected static String insertPreparedStatementString;

  /** {@inheritDoc} */
  @Override
  public void init() throws DBException {
    super.init();

    synchronized (AbstractSqlClient.class) {
      if (readPreparedStatementString != null || insertPreparedStatementString != null) {
        return;
      }

      readPreparedStatementString = String.format("SELECT * FROM %s WHERE %s = ?", cacheName, PRIMARY_COLUMN_NAME);

      List<String> columns = new ArrayList<>(Collections.singletonList(PRIMARY_COLUMN_NAME));
      columns.addAll(FIELDS);

      String columnsString = String.join(", ", columns);

      String valuesString = String.join(", ", Collections.nCopies(columns.size(), "?"));

      insertPreparedStatementString = String.format("INSERT INTO %s (%s) VALUES (%s)",
          cacheName, columnsString, valuesString);
    }
  }

  /**
   * Set values for the prepared statement object.
   *
   * @param statement Prepared statement object.
   * @param key Key field value.
   * @param values Values.
   */
  static void setStatementValues(PreparedStatement statement, String key, Map<String, ByteIterator> values)
      throws SQLException {
    int i = 1;

    statement.setString(i++, key);

    for (String fieldName: FIELDS) {
      statement.setString(i++, values.get(fieldName).toString());
    }
  }
}
