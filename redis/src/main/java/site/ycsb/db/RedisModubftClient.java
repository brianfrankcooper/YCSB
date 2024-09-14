/**
 * Copyright (c) 2012 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

/**
 * Redis client binding for YCSB.
 *
 * All YCSB records are mapped to a Redis *hash field*.  For scanning
 * operations, all keys are saved (by an arbitrary hash) in a sorted set.
 */

 package site.ycsb.db;

 import site.ycsb.ByteIterator;
 import site.ycsb.DB;
 import site.ycsb.DBException;
 import site.ycsb.Status;
 
 import java.io.IOException;
 import java.io.InputStream;
 import java.io.OutputStream;
 import java.net.Socket;
 import java.nio.ByteBuffer;
 import java.util.HashMap;
 import java.util.Map;
 import java.util.Properties;
 import java.util.Set;
 import java.util.Vector;
 
 import queryMessage.Message;
 import queryMessage.Message.Query;
 import responseMessage.ResponseOuterClass.Response;
 
 /**
  * YCSB binding for <a href="http://redis.io/">Redis</a>.
  *
  * See {@code redis/README.md} for details.
  */
 public class RedisModubftClient extends DB {
 
   public static final String INDEX_KEY = "_indices";
   private int moduBftClientPort = 10000;
 
   private Socket activeConnection;
 
   private OutputStream out;
   private InputStream in;
 
   private int tId;
 
   public void init() throws DBException {
     try {
       Properties props = getProperties();
       tId = Integer.parseInt(props.getProperty("threadId"));
       moduBftClientPort += tId;
       activeConnection = new Socket("localhost", moduBftClientPort);
       out = activeConnection.getOutputStream();
       in = activeConnection.getInputStream();
     } catch (Exception e) {
       throw new DBException(
           String.format("Failed to establish  connection with modubft client at port %d", moduBftClientPort), e);
     }
   }
 
   public void cleanup() throws DBException {
     return;
   }
 
   /*
    * Calculate a hash for a key to store it in an index. The actual return value
    * of this function is not interesting -- it primarily needs to be fast and
    * scattered along the whole space of doubles. In a real world scenario one
    * would probably use the ASCII values of the keys.
    */
   private double hash(String key) {
     return key.hashCode();
   }
 
   @Override
   public Status read(String table, String key, Set<String> fields,
       Map<String, ByteIterator> result) {
     Response resp;
     if (fields == null) {
       resp = sendQuery("HGETALL", key);
     } else {
       String[] args = new String[2 + fields.size()];
       String[] fieldsArray = (String[]) fields.toArray();
       args[0] = "HMGET";
       args[1] = key;
 
       for (int i = 2; i < 2 + fields.size(); i++) {
         args[i] = fieldsArray[i - 2];
       }
       resp = sendQuery(args);
     }
     if (resp.getResultCount() > 0) {
       return Status.OK;
     } else {
       return Status.ERROR;
     }
   }
 
   @Override
   public Status insert(String table, String key,
       Map<String, ByteIterator> values) {
     String[] args = new String[values.size() * 2 + 2];
     args[0] = "HMSET";
     args[1] = key;
     int index = 2;
     for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
       args[index++] = entry.getKey();
       args[index++] = entry.getValue().toString();
     }
 
     Response respHmset = sendQuery(args);

 
     if (respHmset.getResult(0).equals("OK")) {
       sendQuery("ZADD", INDEX_KEY, Double.toString(hash(key)), key);
       return Status.OK;
     } else {
       return Status.ERROR;
     }
 
   }
 
   @Override
   public Status delete(String table, String key) {
 
     Response respDel = sendQuery("DEL", key);
 
     if (Integer.parseInt(respDel.getResult(0)) == 0) {
       Response respZrem = sendQuery("ZREM", key);
       if (Integer.parseInt(respZrem.getResult(0)) == 0) {
         return Status.OK;
       }
     }
     return Status.ERROR;
 
   }
 
   @Override
   public Status update(String table, String key,
       Map<String, ByteIterator> values) {
     String[] args = new String[values.size() * 2 + 2];
     args[0] = "HMSET";
     args[1] = key;
     int index = 2;
     for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
       args[index++] = entry.getKey();
       args[index++] = entry.getValue().toString();
     }
     Response resp = sendQuery(args);
     if (resp.getResultCount() > 0 && resp.getResult(0).equals("OK")) {
       return Status.OK;
     } else {
       return Status.ERROR;
     }
 
   }
 
   @Override
   public Status scan(String table, String startkey, int recordcount,
       Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
     throw new UnsupportedOperationException("Scan is not offered yet");
   }
 
   private Response sendQuery(String... args) {
 
     try {
       //System.out.println("sendQuery in thread: " + tId);
       byte[] msg = transformArgsToProtoMessage(args);
 
       byte[] length = ByteBuffer.allocate(4).putInt(msg.length).array();
       out.write(length);
       out.write(msg);
       out.flush();
       return getResponse(args);
 
     } catch (Exception e) {
       e.printStackTrace();
       return Response.newBuilder().build();
     }
   }
 
   private byte[] transformArgsToProtoMessage(String[] args) {
     Message.Query.Builder queryBuilder = Query.newBuilder();
     for (String s : args) {
       queryBuilder.addValues(s);
     }
     Query query = queryBuilder.build();
     byte[] data = query.toByteArray();
     return data;
   }

 
   private Response getResponse(String... args) throws IOException {
     byte[] lengthBuf = new byte[4];
     int bytesRead = 0;
 
     while (bytesRead < 4) {
       int result = in.read(lengthBuf, bytesRead, 4 - bytesRead);
       if (result == -1) {
         if (bytesRead == 0) {
           return null; // end of stream
         } else {
           throw new IOException("Unexpected end of stream");
         }
       }
       bytesRead += result;
     }
     int length = ByteBuffer.wrap(lengthBuf).getInt();
     if (length == 0) {
       throw new IOException("response size can't be 0");
     }
     byte[] messageBytes = new byte[length];
 
     bytesRead = 0;
 
     while (bytesRead < length) {
       int result = in.read(messageBytes, bytesRead, length);
       if (result == -1) {
         throw new IOException("Unexpected end of stream");
       }
       bytesRead += result;
     }
     Response response = Response.parseFrom(messageBytes);
     return response;
 
   }
 
 }