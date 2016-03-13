package com.yahoo.ycsb.db;

import com.ceph.rados.Rados;
import com.ceph.rados.IoCTX;
import com.ceph.rados.jna.RadosObjectInfo;
import com.ceph.rados.ReadOp;
import com.ceph.rados.ReadOp.ReadResult;
import com.ceph.rados.exceptions.RadosException;


import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;

import java.io.File;
import java.util.HashMap;
import java.util.Set;
import java.util.Vector;

import org.json.JSONObject;

/**
 * YCSB binding for <a href="http://redis.io/">Redis</a>.
 *
 * See {@code redis/README.md} for details.
 */
public class RadosClient extends DB {

  // TODO: use conf file
  private static String envCONFIGFILE = System.getenv("RADOS_JAVA_CONFIG_FILE");
  private static String envID = System.getenv("RADOS_JAVA_ID");
  private static String envPOOL = System.getenv("RADOS_JAVA_POOL");

  private static final String CONFIG_FILE = envCONFIGFILE == null ? "/etc/ceph/ceph.conf" : envCONFIGFILE;
  private static final String ID = envID == null ? "admin" : envID;
  private static final String POOL = envPOOL == null ? "data" : envPOOL;

  private static Rados rados;
  private static IoCTX ioctx;

  public void init() throws DBException {
    rados = new Rados(ID);
    try {
      rados.confReadFile(new File(CONFIG_FILE));
      rados.connect();
      ioctx = rados.ioCtxCreate(POOL);
    } catch (RadosException e) {
      throw new DBException(e.getMessage() + ": " + e.getReturnValue());
    }
  }

  public void cleanup() throws DBException {
    rados.shutDown();
    rados.ioCtxDestroy(ioctx);
  }

  @Override
  public Status read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
    byte[] buffer;

    try {
      RadosObjectInfo info = ioctx.stat(key);
      buffer = new byte[(int)info.getSize()];

      ReadOp rop = ioctx.readOpCreate();
      ReadResult readResult = rop.queueRead(0, info.getSize());
      // TODO: more size than byte length possible;
      rop.operate(key, Rados.OPERATION_NOFLAG);
      readResult.raiseExceptionOnError("Error ReadOP(%d)", readResult.getRVal());
      if (info.getSize() != readResult.getBytesRead()) {
        return new Status("ERROR", "Error the object size read");
      }
      readResult.getBuffer().get(buffer);
    } catch (RadosException e) {
      return new Status("ERROR-" + e.getReturnValue(), e.getMessage());
    }

    JSONObject json = new JSONObject(new String(buffer, java.nio.charset.StandardCharsets.UTF_8));
    Set<String> fieldsToReturn = (fields == null ? json.keySet() : fields);

    for (String name : fieldsToReturn) {
      result.put(name, new StringByteIterator(json.getString(name)));
    }

    return result.isEmpty() ? Status.ERROR : Status.OK;
  }

  @Override
  public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
    JSONObject json = new JSONObject(values);

    try {
      ioctx.write(key, json.toString());
    } catch (RadosException e) {
      return new Status("ERROR-" + e.getReturnValue(), e.getMessage());
    }
    return Status.OK;
  }

  @Override
  public Status delete(String table, String key) {
    try {
      ioctx.remove(key);
    } catch (RadosException e) {
      return new Status("ERROR-" + e.getReturnValue(), e.getMessage());
    }
    return Status.OK;
  }

  @Override
  public Status update(String table, String key, HashMap<String, ByteIterator> values) {
    Status rtn = delete(table, key);
    if (rtn.equals(Status.OK)) {
      return insert(table, key, values);
    }
    return rtn;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {
    return Status.NOT_IMPLEMENTED;
  }
}
