package site.ycsb.db;

import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.MultiCheckpointWriter;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import site.ycsb.ByteArrayByteIterator;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * YCSB binding for CorfuDB.
 */
public class CorfuDBClient extends DB {

  private static final String CONNECTION_STRING = "corfu.connection";
  private static final String CLIENT_MODE = "corfu.singleclient";
  private static final String TABLE_NAME = "usertable";

  private final int numTxnRetry = 3;

  private static final Logger LOGGER = LoggerFactory.getLogger(CorfuDBClient.class);

  private CorfuRuntime corfuRuntime;
  private CorfuTable<String, byte[]> userTable;

  private static final AtomicInteger THREAD_COUNT = new AtomicInteger(0);

  private static CorfuRuntime singletonRt;

  private void initCheckpointer(String connectionStr) {
    ThreadFactory namedThreadFactory =
        new ThreadFactoryBuilder().setNameFormat("corfu-checkpointer-%d").build();
    ExecutorService executorService = Executors.newSingleThreadExecutor(namedThreadFactory);
    executorService.submit(() -> {
        try {
          while (true) {
            CorfuRuntime cpRt = new CorfuRuntime(connectionStr).connect();
            CorfuTable<String, byte[]> tableToCheckpoint = cpRt.getObjectsView()
                .build()
                .setTypeToken(new TypeToken<CorfuTable<String, byte[]>>() {
                })
                .setStreamName(TABLE_NAME)
                .open();

            long start = System.currentTimeMillis();
            MultiCheckpointWriter<CorfuTable> mcw = new MultiCheckpointWriter<>();
            mcw.addMap(tableToCheckpoint);
            Token trimPoint = mcw.appendCheckpoints(cpRt, "Checkpointer-thread");
            long end = System.currentTimeMillis();
            long sleepTime = 1000 * 60 * 2;
            System.out.println(">>>> Checkpointer took " + (end - start) + " ms sleeping for " + sleepTime + "seconds");
            Thread.sleep(sleepTime);
            cpRt.getAddressSpaceView().prefixTrim(trimPoint);
            cpRt.getAddressSpaceView().gc();
            cpRt.shutdown();
          }
        } catch (Exception e) {
          System.out.println("Checkpointer encountered an error! " + e);
          Runtime.getRuntime().halt(-1);
        }
      });
  }

  @Override
  public void init() throws DBException {
    Properties props = getProperties();

    boolean singleMode = Boolean.parseBoolean(props.getProperty(CLIENT_MODE, "false"));

    if (THREAD_COUNT.incrementAndGet() == 1) {
      initCheckpointer(props.getProperty(CONNECTION_STRING));
    }

    synchronized (THREAD_COUNT) {
      if (singleMode) {
        // Share the same CorfuRuntime across workers
        if (singletonRt == null) {
          singletonRt = new CorfuRuntime(props.getProperty(CONNECTION_STRING)).connect();
        }

        corfuRuntime = singletonRt;
      } else {
        // Create a new CorfuRuntime per worker
        corfuRuntime = new CorfuRuntime(props.getProperty(CONNECTION_STRING)).connect();
      }
    }

    // Since NO_CACHE option is not specified this will retrieve one table instance per CorfuRuntime
    userTable = corfuRuntime.getObjectsView()
        .build()
        .setTypeToken(new TypeToken<CorfuTable<String, byte[]>>() {
        })
        .setStreamName(TABLE_NAME)
        .open();

    long start = System.currentTimeMillis();
    int size = userTable.size();
    System.out.println("Table: " + TABLE_NAME + " size: " + size + " ref: " + System.identityHashCode(userTable)
        + " loadTime: " + (System.currentTimeMillis() - start));
    LOGGER.info("connected!");
  }

  @Override
  public void cleanup() throws DBException {
    System.out.println("[Table] " + TABLE_NAME + " size " + userTable.size()
        + " " + System.identityHashCode(userTable));
    if (THREAD_COUNT.decrementAndGet() == 0) {
      corfuRuntime.shutdown();
    }
  }

  private CorfuTable<String, byte[]> getTable(String tableName) {
    if (!tableName.equals(TABLE_NAME)) {
      throw new IllegalStateException("unknown table " + tableName);
    }

    return userTable;
  }

  private byte[] serializeValue(Map<String, ByteIterator> value) {
    ByteBuf buf = Unpooled.buffer();
    buf.writeInt(value.size());

    for (Map.Entry<String, ByteIterator> entry : value.entrySet()) {
      String key = entry.getKey();
      ByteIterator val = entry.getValue();

      buf.writeInt(key.length());
      buf.writeCharSequence(key, StandardCharsets.UTF_8);

      buf.writeInt(value.size());
      // TODO(Maithem): remove this extra copy
      buf.writeBytes(val.toArray());
    }

    // TODO(Maithem): Does this array need to be sliced?
    return buf.array();
  }

  private Map<String, ByteIterator> deserializeValue(byte[] value) {
    ByteBuf buf = Unpooled.wrappedBuffer(value);
    int items = buf.readInt();

    Map<String, ByteIterator> retValue = new HashMap<>(items);

    for (int x = 0; x < items; x++) {
      int keyLen = buf.readInt();
      String key = buf.readCharSequence(keyLen, StandardCharsets.UTF_8).toString();

      int valLen = buf.readInt();
      byte[] payload = new byte[valLen];
      buf.readBytes(payload);

      ByteIterator byteIterator = new ByteArrayByteIterator(payload);
      ByteIterator oldVal = retValue.put(key, byteIterator);

      if (oldVal != null) {
        throw new IllegalStateException("Duplicate key/val");
      }
    }
    return retValue;
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    try {
      CorfuTable<String, byte[]> corfuTable = getTable(table);
      byte[] value = corfuTable.get(key);
      if (value == null) {
        return Status.NOT_FOUND;
      }

      for (Map.Entry<String, ByteIterator>  entry : deserializeValue(value).entrySet()) {
        if (fields == null || fields.contains(entry.getKey())) {
          result.put(entry.getKey(), entry.getValue());
        }
      }

      return Status.OK;
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {

    // Since Corfu is just a hashtable seek and iterate are not supported, so we just scan the whole
    // table instead.
    CorfuTable<String, byte[]> corfuTable = getTable(table);
    corfuTable.scanAndFilter(entry -> true);
    return Status.OK;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    return insert(table, key, values);
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    CorfuTable<String, byte[]> corfuTable = getTable(table);
    for (int attempt = 0; attempt < numTxnRetry; attempt++) {
      try {
        corfuRuntime.getObjectsView().TXBegin();
        corfuTable.put(key, serializeValue(values));
        corfuRuntime.getObjectsView().TXEnd();
        return Status.OK;
      } catch (TransactionAbortedException tae) {
        LOGGER.warn("Txn Abort attempt {}", attempt, tae);
      } catch (Exception e) {
        LOGGER.error(e.getMessage(), e);
        return Status.ERROR;
      }
    }
    return Status.ERROR;
  }

  @Override
  public Status delete(String table, String key) {
    CorfuTable<String, byte[]> corfuTable = getTable(table);
    for (int attempt = 0; attempt < numTxnRetry; attempt++) {
      try {
        corfuRuntime.getObjectsView().TXBegin();
        corfuTable.remove(key);
        corfuRuntime.getObjectsView().TXEnd();
        return Status.OK;
      } catch (TransactionAbortedException tae) {
        LOGGER.warn("Txn Abort attempt {}", attempt, tae);
      } catch (Exception e) {
        LOGGER.error(e.getMessage(), e);
        return Status.ERROR;
      }
    }
    return Status.ERROR;
  }

}
