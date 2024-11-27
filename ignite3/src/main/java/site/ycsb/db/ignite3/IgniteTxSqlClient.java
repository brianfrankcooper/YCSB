package site.ycsb.db.ignite3;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.tx.Transaction;
import org.apache.ignite.tx.TransactionException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import site.ycsb.ByteIterator;
import site.ycsb.Status;

/**
 * Ignite3 SQL API client with using transactions.
 */
public class IgniteTxSqlClient extends IgniteSqlClient {

  private static final Logger LOG = LogManager.getLogger(IgniteTxSqlClient.class);

  /** Transaction. */
  private Transaction tx;

  /** {@inheritDoc} */
  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    try {
      Status status;

      tx = ignite.transactions().begin();

      status = get(tx, table, key, fields, result);

      tx.commit();

      return status;
    } catch (TransactionException txEx) {
      LOG.error("Error reading key in transaction. Calling rollback.", txEx);
      tx.rollback();

      throw txEx;
    } catch (Exception e) {
      LOG.error(String.format("Error reading key: %s", key), e);

      return Status.ERROR;
    }
  }

  /** {@inheritDoc} */
  @Override
  public Status batchRead(String table, List<String> keys, List<Set<String>> fields,
                          List<Map<String, ByteIterator>> results) {
    try {
      tx = ignite.transactions().begin();

      for (int i = 0; i < keys.size(); i++) {
        HashMap<String, ByteIterator> result = new HashMap<>();

        Status status = get(tx, table, keys.get(i), fields.get(i), result);

        if (status != Status.OK) {
          throw new TransactionException(-1, String.format("Unable to read key %s", keys.get(i)));
        }

        results.add(result);
      }

      tx.commit();

      return Status.OK;
    } catch (TransactionException txEx) {
      LOG.error("Error reading batch of keys in transaction. Calling rollback.", txEx);
      tx.rollback();

      throw txEx;
    } catch (Exception e) {
      LOG.error("Error reading batch of keys.", e);

      return Status.ERROR;
    }
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
      tx = ignite.transactions().begin();

      put(tx, key, values);

      tx.commit();

      return Status.OK;
    } catch (TransactionException txEx) {
      LOG.error("Error inserting key in transaction. Calling rollback.", txEx);
      tx.rollback();

      throw txEx;
    } catch (Exception e) {
      LOG.error(String.format("Error inserting key: %s", key), e);

      return Status.ERROR;
    }
  }

  /** {@inheritDoc} */
  @Override
  public Status batchInsert(String table, List<String> keys, List<Map<String, ByteIterator>> values) {
    try {
      tx = ignite.transactions().begin();

      for (int i = 0; i < keys.size(); i++) {
        put(tx, keys.get(i), values.get(i));
      }

      tx.commit();

      return Status.OK;
    } catch (TransactionException txEx) {
      LOG.error("Error inserting batch of keys in transaction. Calling rollback.", txEx);
      tx.rollback();

      throw txEx;
    } catch (Exception e) {
      LOG.error("Error inserting batch of keys.", e);

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

      tx = ignite.transactions().begin();

      ignite.sql().execute(tx, deleteStatement).close();

      tx.commit();

      return Status.OK;
    } catch (TransactionException txEx) {
      LOG.error("Error deleting key in transaction. Calling rollback.", txEx);
      tx.rollback();

      throw txEx;
    } catch (Exception e) {
      LOG.error(String.format("Error deleting key: %s ", key), e);

      return Status.ERROR;
    }
  }
}
