package site.ycsb.db.ignite3;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.sql.Statement;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import site.ycsb.ByteIterator;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;

/**
 * Ignite3 SQL API client.
 */
public class IgniteSqlClient extends AbstractSqlClient {

  private static final Logger LOG = LogManager.getLogger(IgniteSqlClient.class);

  private static final AtomicInteger SQL_INIT_COUNT = new AtomicInteger(0);

  /** Statement for reading values. */
  private static final ThreadLocal<Statement> READ_STATEMENT = ThreadLocal
      .withInitial(IgniteSqlClient::buildReadStatement);

  /** Statement for inserting values. */
  private static final ThreadLocal<Statement> INSERT_STATEMENT = ThreadLocal
      .withInitial(IgniteSqlClient::buildInsertStatement);

  /** Build statement for reading values. */
  private static Statement buildReadStatement() {
    return ignite.sql().createStatement(readPreparedStatementString);
  }

  /** Build statement for inserting values. */
  private static Statement buildInsertStatement() {
    return ignite.sql().createStatement(insertPreparedStatementString);
  }

  /** {@inheritDoc} */
  @Override
  public void init() throws DBException {
    super.init();

    SQL_INIT_COUNT.incrementAndGet();
  }

  /** {@inheritDoc} */
  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    try {
      try (ResultSet<SqlRow> rs = ignite.sql().execute(null, READ_STATEMENT.get(), key)) {
        if (!rs.hasNext()) {
          return Status.NOT_FOUND;
        }

        SqlRow row = rs.next();

        if (fields == null || fields.isEmpty()) {
          fields = new HashSet<>();
          fields.addAll(this.fields);
        }

        for (String column : fields) {
          // TODO: this does not work. A bug?
          // String val = row.stringValue(column);

          // Shift to exclude the first column from the result
          String val = row.stringValue(this.fields.indexOf(column) + 1);

          if (val != null) {
            result.put(column, new StringByteIterator(val));
          }
        }
      }

      if (debug) {
        LOG.info("table: {}, key: {}, fields: {}", table, key, fields);
        LOG.info("result: {}", result);
      }
    } catch (Exception e) {
      LOG.error(String.format("Error reading key: %s", key), e);

      return Status.ERROR;
    }

    return Status.OK;
  }

  /** {@inheritDoc} */
  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    return Status.NOT_IMPLEMENTED;
  }

  /** {@inheritDoc} */
  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    try {
      List<String> valuesList = new ArrayList<>();
      valuesList.add(key);
      fields.forEach(fieldName -> valuesList.add(String.valueOf(values.get(fieldName))));
      ignite.sql().execute(null, INSERT_STATEMENT.get(), (Object[]) valuesList.toArray(new String[0])).close();

      return Status.OK;
    } catch (Exception e) {
      LOG.error(String.format("Error inserting key: %s", key), e);

      return Status.ERROR;
    }
  }

  /** {@inheritDoc} */
  @Override
  public Status delete(String table, String key) {
    String deleteStatement = String.format(
        "DELETE FROM %s WHERE %s = '%s'", table, PRIMARY_COLUMN_NAME, key
    );

    try {
      if (debug) {
        LOG.info(deleteStatement);
      }

      ignite.sql().execute(null, deleteStatement).close();

      return Status.OK;
    } catch (Exception e) {
      LOG.error(String.format("Error deleting key: %s ", key), e);
    }

    return Status.ERROR;
  }
}
