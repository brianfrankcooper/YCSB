package com.yahoo.ycsb.db;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import com.yahoo.mapkeeper.BinaryResponse;
import com.yahoo.mapkeeper.MapKeeper;
import com.yahoo.mapkeeper.Record;
import com.yahoo.mapkeeper.RecordListResponse;
import com.yahoo.mapkeeper.ResponseCode;
import com.yahoo.mapkeeper.ScanOrder;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.StringByteIterator;
import com.yahoo.ycsb.workloads.CoreWorkload;

public class MapKeeperClient extends DB {
    private static final String HOST = "mapkeeper.host";
    private static final String HOST_DEFAULT = "localhost";
    private static final String PORT = "mapkeeper.port";
    private static final String PORT_DEFAULT = "9090";
    MapKeeper.Client c; 
    boolean writeallfields;
    static boolean initteddb = false;
    private synchronized static void initDB(Properties p, MapKeeper.Client c) throws TException {
        if(!initteddb) {
            initteddb = true;
            c.addMap(p.getProperty(CoreWorkload.TABLENAME_PROPERTY, CoreWorkload.TABLENAME_PROPERTY_DEFAULT));
        }
    }

    public void init() {
        String host = getProperties().getProperty(HOST, HOST_DEFAULT);
        int port = Integer.parseInt(getProperties().getProperty(PORT, PORT_DEFAULT));
        TTransport tr = new TFramedTransport(new TSocket(host, port));
        TProtocol proto = new TBinaryProtocol(tr);
        c = new MapKeeper.Client(proto);
        try {
            tr.open();
            initDB(getProperties(), c);
        } catch(TException e) {
            throw new RuntimeException(e);
        }
        writeallfields = Boolean.parseBoolean(getProperties().getProperty(CoreWorkload.WRITE_ALL_FIELDS_PROPERTY, 
                    CoreWorkload.WRITE_ALL_FIELDS_PROPERTY_DEFAULT));
    }

    ByteBuffer encode(HashMap<String, ByteIterator> values) {
        int len = 0;
        for(String k : values.keySet()) {
            len += (k.length() + 1 + values.get(k).bytesLeft() + 1);
        }
        byte[] array = new byte[len];
        int i = 0;
        for(String k : values.keySet()) {
            for(int j = 0; j < k.length(); j++) {
                array[i] = (byte)k.charAt(j);
                i++;
            }
            array[i] = '\t'; // XXX would like to use sane delimiter (null, 254, 255, ...) but java makes this nearly impossible
            i++;
            ByteIterator v = values.get(k);
            i = v.nextBuf(array, i);
            array[i] = '\t';
            i++;
        }
        array[array.length-1] = 0;
        ByteBuffer buf = ByteBuffer.wrap(array);
        buf.rewind();
        return buf;
    }
    void decode(Set<String> fields, String tups, HashMap<String, ByteIterator> tup) {
        String[] tok = tups.split("\\t");
        if(tok.length == 0) { throw new IllegalStateException("split returned empty array!"); }
        for(int i = 0; i < tok.length; i+=2) {
            if(fields == null || fields.contains(tok[i])) {
                if(tok.length < i+2) { throw new IllegalStateException("Couldn't parse tuple <" + tups + "> at index " + i); }
                if(tok[i] == null || tok[i+1] == null) throw new NullPointerException("Key is " + tok[i] + " val is + " + tok[i+1]);
                tup.put(tok[i], new StringByteIterator(tok[i+1]));
            }
        }
        if(tok.length == 0) {
            System.err.println("Empty tuple: " + tups);
        }
    }

    int ycsbThriftRet(BinaryResponse succ, ResponseCode zero, ResponseCode one) {
        return ycsbThriftRet(succ.responseCode, zero, one);
    }
    int ycsbThriftRet(ResponseCode rc, ResponseCode zero, ResponseCode one) {
        return
            rc == zero ? 0 :
            rc == one  ? 1 : 2;
    }
    ByteBuffer bufStr(String str) {
        ByteBuffer buf = ByteBuffer.wrap(str.getBytes());
        return buf;
    }
    String strResponse(BinaryResponse buf) {
        return new String(buf.value.array());
    }

    @Override
    public int read(String table, String key, Set<String> fields,
            HashMap<String, ByteIterator> result) {
        try {
            ByteBuffer buf = bufStr(key);

            BinaryResponse succ = c.get(table, buf);

            int ret = ycsbThriftRet(
                    succ,
                    ResponseCode.RecordExists,
                    ResponseCode.RecordNotFound);

            if(ret == 0) {
                decode(fields, strResponse(succ), result);
            }
            return ret;
        } catch(TException e) {
            e.printStackTrace();
            return 2;
        }
    }

    @Override
    public int scan(String table, String startkey, int recordcount,
            Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        try {
            //XXX what to pass in for nulls / zeros?
            RecordListResponse res = c.scan(table, ScanOrder.Ascending, bufStr(startkey), true, null, false, recordcount, 0);
            int ret = ycsbThriftRet(res.responseCode, ResponseCode.Success, ResponseCode.ScanEnded);
            if(ret == 0) {
                for(Record r : res.records) {
                    HashMap<String, ByteIterator> tuple = new HashMap<String, ByteIterator>();
                    // Note: r.getKey() and r.getValue() call special helper methods that trim the buffer
                    // to an appropriate length, and memcpy it to a byte[].  Trying to manipulate the ByteBuffer
                    // directly leads to trouble.
                    tuple.put("key", new StringByteIterator(new String(r.getKey())));
                    decode(fields, new String(r.getValue())/*strBuf(r.bufferForValue())*/, tuple);
                    result.add(tuple);
                }
            }
            return ret;
        } catch(TException e) {
            e.printStackTrace();
            return 2;
        }
    }

    @Override
    public int update(String table, String key,
            HashMap<String, ByteIterator> values) {
        try {
            if(!writeallfields) {
                HashMap<String, ByteIterator> oldval = new HashMap<String, ByteIterator>();
                read(table, key, null, oldval);
                for(String k: values.keySet()) {
                    oldval.put(k, values.get(k));
                }
                values = oldval;
            }
            ResponseCode succ = c.update(table, bufStr(key), encode(values));
            return ycsbThriftRet(succ, ResponseCode.RecordExists, ResponseCode.RecordNotFound);
        } catch(TException e) {
            e.printStackTrace();
            return 2;
        }
    }

    @Override
    public int insert(String table, String key,
            HashMap<String, ByteIterator> values) {
        try {
            int ret = ycsbThriftRet(c.insert(table, bufStr(key), encode(values)), ResponseCode.Success, ResponseCode.RecordExists);
            return ret;
        } catch(TException e) {
            e.printStackTrace();
            return 2;
        }
    }

    @Override
    public int delete(String table, String key) {
        try {
            return ycsbThriftRet(c.remove(table, bufStr(key)), ResponseCode.Success, ResponseCode.RecordExists);
        } catch(TException e) {
            e.printStackTrace();
            return 2;
        }
    }
}
