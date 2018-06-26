import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import org.rocksdb.*;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Created by Yosub on 10/2/14.
 * Modified by whb on 2017.10
 */
public class RocksdbClient extends DB {

  static {
    RocksDB.loadLibrary();
  }

  private static final String DB_PATH = "/tmp/rocksdb_slot_modified_statsdump_100s_ycsb_1G_60G_onlyrun";
  private static final int BYTE_BUFFER_SIZE = 4096;


  private Date date;
  private RocksDB db;
  private Options options;

  public void init() throws DBException {
    System.out.println("Initializing RocksDB...");
    date = new Date();
    String dbPath = DB_PATH;
    options = new Options();
    options.setCreateIfMissing(true);
    options.setStatsDumpPeriodSec(100);
    options.setCompressionType(CompressionType.NO_COMPRESSION);
    System.out.println("options.statsDumpPeriodSec() = " + options.statsDumpPeriodSec());
    try {
      db = RocksDB.open(options, dbPath);
    } catch (RocksDBException e) {
      System.out.format("[ERROR] caught the unexpceted exception -- %s\n", e);
      assert(false);
    }

    System.out.println("Initializing RocksDB is over");
  }

  public void cleanup() throws DBException {
    super.cleanup();
    try {
      System.out.println("end operation");
      String str = db.getProperty("rocksdb.stats");
      System.out.println(str);
   //   System.out.println("Begin full compaction");
   //   db.compactRange();
   //   str = db.getProperty("rocksdb.stats");
   //   System.out.println(str);
    } catch (RocksDBException e) {
      throw new DBException("Error while trying to print RocksDB statistics");
    }
    System.out.println("Beginning Sleep...");
    try{
      for(int i=1; i<=10; i++){
        System.out.println("begin the "+i+" interval :");
        Thread.sleep(1000*60);
        String str = db.getProperty("rocksdb.stats");
        System.out.println("after "+i+" mins");
        System.out.println(str);
      }
    } catch (RocksDBException e) {
      throw new DBException("Error while trying to print RocksDB statistics while sleeping");
    } catch (InterruptedException e){
      e.printStackTrace();
      System.out.println("Exception!!!!  sleep found exception");
    }
    System.out.println("Disconnecting RocksDB database...");
    db.close();
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    try {
      byte[] value = db.get(key.getBytes());
      Map<String, ByteIterator> deserialized = deserialize(value);
      result.putAll(deserialized);
    } catch (RocksDBException e) {
      System.out.format("[ERROR] caught the unexpceted exception -- %s\n", e);
      return Status.ERROR;
    }
    return Status.OK;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount,
                  Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    System.out.println("Scan called! NOP for now");
    return Status.OK;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    try {
      byte[] serialized = serialize(values);
      db.put(key.getBytes(), serialized);
    } catch (RocksDBException e) {
      System.out.format("[ERROR] caught the unexpceted exception -- %s\n", e);
      return Status.ERROR;
    }
    return Status.OK;
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    try {
      byte[] serialized = serialize(values);
      db.put(key.getBytes(), serialized);
    } catch (RocksDBException e) {
      System.out.format("[ERROR] caught the unexpceted exception -- %s\n", e);
      return Status.ERROR;
    }
    return Status.OK;
  }

  @Override
  public Status delete(String table, String key) {
    try {
      db.remove(key.getBytes());
    } catch (RocksDBException e) {
      System.out.format("[ERROR] caught the unexpceted exception -- %s\n", e);
      return Status.ERROR;
    }
    return Status.OK;
  }

  private byte[] serialize(Map<String, ByteIterator> values) {
    ByteBuffer buf = ByteBuffer.allocate(BYTE_BUFFER_SIZE);
    // Number of elements in HashMap (int)
    buf.put((byte) values.size());
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      // Key string length (int)
      buf.put((byte) entry.getKey().length());
      // Key bytes
      buf.put(entry.getKey().getBytes());
      // Value bytes length (long)
      buf.put((byte) entry.getValue().bytesLeft());
      // Value bytes
      buf.put((entry.getValue().toArray()));
    }

    byte[] result = new byte[buf.position()];
    buf.get(result, 0, buf.position());
    return result;
  }

  private HashMap<String, ByteIterator> deserialize(byte[] bytes) {
    HashMap<String, ByteIterator> result = new HashMap<String, ByteIterator>();
    ByteBuffer buf = ByteBuffer.wrap(bytes);
    int count = buf.getInt();
    for (int i = 0; i < count; i++) {
      int keyLength = buf.getInt();
      byte[] keyBytes = new byte[keyLength];
      buf.get(keyBytes, buf.position(), keyLength);

      int valueLength = buf.getInt();
      byte[] valueBytes = new byte[valueLength];
      buf.get(valueBytes, buf.position(), valueLength);

      result.put(new String(keyBytes), new ByteArrayByteIterator(valueBytes));
    }
    return result;
  }
}
