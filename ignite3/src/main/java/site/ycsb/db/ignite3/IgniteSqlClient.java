package site.ycsb.db.ignite3;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.Session;
import org.apache.ignite.sql.SqlRow;
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

  protected static Session session;

  private static final AtomicInteger SQL_INIT_COUNT = new AtomicInteger(0);

  @Override
  public void init() throws DBException {
    super.init();

    SQL_INIT_COUNT.incrementAndGet();

    synchronized (IgniteSqlClient.class) {
      if (session == null) {
        session = node.sql().createSession();
      }
    }
  }

  /** {@inheritDoc} */
  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    try {
      String qry = prepareReadStatement(table, key);

      if (debug) {
        LOG.info(qry);
      }

      try (ResultSet<SqlRow> rs = session.execute(null, qry)) {
        SqlRow row = rs.next();

        if (row == null) {
          return Status.NOT_FOUND;
        }

        if (fields == null || fields.isEmpty()) {
          fields = new HashSet<>();
          fields.addAll(FIELDS);
        }

        for (String column : fields) {
          // TODO: this does not work. A bug?
          // String val = row.stringValue(column);

          // Shift to exclude the first column from the result
          String val = row.stringValue(FIELDS.indexOf(column) + 1);

          if (val != null) {
            result.put(column, new StringByteIterator(val));
          }
        }
      }

      if (debug) {
        LOG.info("table:{" + table + "}, key:{" + key + "}" + ", fields:{" + fields + "}");
        LOG.info("result {" + result + "}");
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
      String insertStatement = prepareInsertStatement(table, key, values);

      if (table.equals(cacheName)) {
        if (debug) {
          LOG.info(insertStatement);
        }

        session.execute(null, insertStatement).close();
      } else {
        throw new UnsupportedOperationException("Unexpected table name: " + table);
      }

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

      session.execute(null, deleteStatement).close();

      return Status.OK;
    } catch (Exception e) {
      LOG.error(String.format("Error deleting key: %s ", key), e);
    }

    return Status.ERROR;
  }

  @Override
  public void cleanup() throws DBException {
    synchronized (IgniteSqlClient.class) {
      int currInitCount = SQL_INIT_COUNT.decrementAndGet();

      if (currInitCount <= 0) {
        try {
          session.close();
          session = null;
        } catch (Exception e) {
          throw new DBException(e);
        }
      }
    }

    super.cleanup();
  }
}
