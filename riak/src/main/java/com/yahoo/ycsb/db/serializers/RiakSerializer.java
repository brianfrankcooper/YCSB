package com.yahoo.ycsb.db.serializers;

import com.basho.riak.client.IRiakObject;
import com.yahoo.ycsb.ByteIterator;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public interface RiakSerializer {
    //private byte[] updateJson(IRiakObject object, Map<String, ByteIterator> values) throws IOException;
    //private byte[] jsonToBytes(Map<String, ByteIterator> values)  throws IOException;

    public byte[] documentToRiak(Map<String, ByteIterator> result)
            throws IOException;

    public void documentFromRiak(IRiakObject object, Set<String> fields, Map<String, ByteIterator> result)
            throws IOException;

    public byte[] updateDocumentFromRiak(IRiakObject object, Map<String, ByteIterator> values)
            throws IOException;

    public HashMap<String,ByteIterator> rowFromRiakScan(IRiakObject row, Set<String> fields, HashMap<String, ByteIterator> rowResult)
        throws IOException;
}
