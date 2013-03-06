package com.yahoo.ycsb.db.serializers;


import com.basho.riak.client.IRiakObject;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.StringByteIterator;
import com.yahoo.ycsb.db.Constants;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class RiakJsonSerializer implements RiakSerializer {
    ObjectMapper om = new ObjectMapper();

    @Override
    public byte[] documentToRiak(Map<String, ByteIterator> values) throws IOException {
        ObjectNode objNode = om.createObjectNode();
        for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
            String key = entry.getKey();
            objNode.put(key, entry.getValue().toString());
        }
        return objNode.toString().getBytes(Constants.CHARSET_UTF8);
    }

    @Override
    public void documentFromRiak(IRiakObject object, Set<String> fields, Map<String, ByteIterator> result) throws IOException {
        //String contentType = object.getContentType();
        //Charset theCharSet = CharsetUtils.getCharset(contentType);
        byte[] data = object.getValue();
        //String dataInCharset = CharsetUtils.asString(data, theCharSet);
        JsonNode jsonNode = om.readTree(data);
        if (fields != null) {
            // return a subset of all available fields in the json node
            for (String field : fields) {
                JsonNode f = jsonNode.get(field);
                result.put(field, new StringByteIterator(f.toString()));
            }
        } else {
            // no fields specified, just return them all
            Iterator<Map.Entry<String, JsonNode>> jsonFields = jsonNode.fields();
            while (jsonFields.hasNext()) {
                Map.Entry<String, JsonNode> field = jsonFields.next();
                result.put(field.getKey(), new StringByteIterator(field.getValue().toString()));
            }
        }
    }

    @Override
    public HashMap<String, ByteIterator> rowFromRiakScan(IRiakObject row, Set<String> fields, HashMap<String, ByteIterator> rowResult)
            throws IOException {

        //String contentType = object.getContentType();
        //Charset charSet = CharsetUtils.getCharset(contentType);
        byte[] data = row.getValue();
        //String dataInCharset = CharsetUtils.asString(data, charSet);
        JsonNode jsonNode = om.readTree(data);
        if (fields != null) {
            // return a subset of all available fields in the json node
            for (String field : fields) {
                JsonNode f = jsonNode.get(field);
                rowResult.put(field, new StringByteIterator(f.toString()));
            }
        } else {
            // no fields specified, just return them all
            Iterator<Map.Entry<String, JsonNode>> jsonFields = jsonNode.fields();
            while (jsonFields.hasNext()) {
                Map.Entry<String, JsonNode> field = jsonFields.next();
                rowResult.put(field.getKey(), new StringByteIterator(field.getValue().toString()));
            }
        }

        return new HashMap<String, ByteIterator>();
    }

    @Override
    public byte[] updateDocumentFromRiak(IRiakObject object, Map<String, ByteIterator> values) throws IOException {
        //String contentType = object.getContentType();
        //Charset charSet = CharsetUtils.getCharset(contentType);
        byte[] data = object.getValue();
        //String dataInCharset = CharsetUtils.asString(data, charSet);
        JsonNode jsonNode = om.readTree(data);
        for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
            ((ObjectNode) jsonNode).put(entry.getKey(), entry.getValue().toString());
        }
        return jsonNode.toString().getBytes(Constants.CHARSET_UTF8);
    }

}
