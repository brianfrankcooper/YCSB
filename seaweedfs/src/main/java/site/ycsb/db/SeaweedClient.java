/**
 * Copyright (c) 2015 YCSB contributors. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 * <p>
 * SeaweedFS storage client binding for YCSB.
 */
package site.ycsb.db;

import seaweedfs.client.*;
import site.ycsb.*;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.*;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

/**
 * SeaweedFS Storage client for YCSB framework.
 *
 * The size of the file to upload is determined by two parameters:
 * - fieldcount this is the number of fields of a record in YCSB
 * - fieldlength this is the size in bytes of a single field in the record
 * together these two parameters define the size of the file to upload,
 * the size in bytes is given by the fieldlength multiplied by the fieldcount.
 * The name of the file is determined by the parameter key.
 * This key is automatically generated by YCSB.
 */
public class SeaweedClient extends DB {

  protected static final ObjectMapper MAPPER = new ObjectMapper();

  private FilerClient filerClient;
  private FilerGrpcClient filerGrpcClient;
  private String filerHost;
  private int filerPort;
  private String folder;

  /**
   * Cleanup any state for this storage.
   * Called once per instance;
   */
  @Override
  public void cleanup() throws DBException {
  }

  /**
   * Delete a file from SeaweedFS Storage.
   *
   * @param tableName The name of the table
   * @param key  The record key of the file to delete.
   * @return OK on success, otherwise ERROR. See the
   * {@link DB} class's description for a discussion of error codes.
   */
  @Override
  public Status delete(String tableName, String key) {
    if (!filerClient.rm(this.folder + "/" + tableName + "/" + key, true, true)) {
      return Status.ERROR;
    }
    return Status.OK;
  }

  /**
   * Initialize any state for the storage.
   * Called once per SeaweedFS instance; If the client is not null it is re-used.
   */
  @Override
  public void init() throws DBException {
    filerHost = getProperties().getProperty("seaweed.filerHost", "localhost");
    filerPort = Integer.parseInt(getProperties().getProperty("seaweed.filerPort", "8888"));
    folder = getProperties().getProperty("seaweed.folder", "/ycsb");
    filerGrpcClient = new FilerGrpcClient(filerHost, filerPort+10000);
    filerClient = new FilerClient(filerGrpcClient);
    filerClient.mkdirs(this.folder, 0755);
  }

  /**
   * Create a new File in the table. Any field/value pairs in the specified
   * values HashMap will be written into the file with the specified record
   * key.
   *
   * @param tableName The name of the table
   * @param key  The record key of the file to insert.
   * @param values A HashMap of field/value pairs to insert in the file.
   *         Only the content of the first field is written to a byteArray
   *         multiplied by the number of field. In this way the size
   *         of the file to upload is determined by the fieldlength
   *         and fieldcount parameters.
   * @return OK on success, ERROR otherwise. See the
   * {@link DB} class's description for a discussion of error codes.
   */
  @Override
  public Status insert(String tableName, String key,
             Map<String, ByteIterator> values) {
    return writeToStorage(tableName, key, values);
  }

  /**
   * Read a file from the table. Each field/value pair from the result
   * will be stored in a HashMap.
   *
   * @param tableName The name of the table
   * @param key  The record key of the file to read.
   * @param fields The list of fields to read, or null for all of them,
   *         it is null by default
   * @param result A HashMap of field/value pairs for the result
   * @return OK on success, ERROR otherwise.
   */
  @Override
  public Status read(String tableName, String key, Set<String> fields,
             Map<String, ByteIterator> result) {
    return readFromStorage(tableName, key, fields, result);
  }

  /**
   * Update a file in the table. Any field/value pairs in the specified
   * values HashMap will be written into the file with the specified file
   * key, overwriting any existing values with the same field name.
   *
   * @param tableName The name of the table
   * @param key  The file key of the file to write.
   * @param values A HashMap of field/value pairs to update in the record
   * @return OK on success, ERORR otherwise.
   */
  @Override
  public Status update(String tableName, String key,
             Map<String, ByteIterator> values) {
    return writeToStorage(tableName, key, values);
  }

  /**
   * Perform a range scan for a set of files in the table. Each
   * field/value pair from the result will be stored in a HashMap.
   *
   * @param tableName The name of the table
   * @param startkey  The file key of the first file to read.
   * @param recordcount The number of files to read
   * @param fields    The list of fields to read, or null for all of them
   * @param result    A Vector of HashMaps, where each HashMap is a set field/value
   *          pairs for one file
   * @return OK on success, ERROR otherwise.
   */
  @Override
  public Status scan(String tableName, String startkey, int recordcount,
             Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    return scanFromStorage(tableName, startkey, recordcount, fields, result);
  }

  /**
   * Write a new object to SeaweedFS.
   *
   * @param tableName The name of the table
   * @param key  The file key of the object to upload/update.
   * @param values The data to be written on the object
   */
  protected Status writeToStorage(String tableName, String key, Map<String, ByteIterator> values) {
    try {
      byte[] jsonData = toJson(values).getBytes(StandardCharsets.UTF_8);

      long now = System.currentTimeMillis() / 1000L;
      FilerProto.Entry.Builder entry = FilerProto.Entry.newBuilder()
              .setName(key)
              .setIsDirectory(false)
              .setAttributes(
                      FilerProto.FuseAttributes.newBuilder()
                              .setCrtime(now)
                              .setMtime(now)
                              .setFileMode(0755)
              );

      SeaweedWrite.writeData(entry, "000", this.filerGrpcClient, 0, jsonData, 0, jsonData.length);

      SeaweedWrite.writeMeta(this.filerGrpcClient, this.folder + "/" + tableName, entry);

    } catch (Exception e) {
      System.err.println("Not possible to write the object " + key);
      e.printStackTrace();
      return Status.ERROR;
    }

    return Status.OK;
  }

  /**
   * Download an object from SeaweedFS.
   *
   * @param tableName The name of the table
   * @param key  The file key of the object to upload/update.
   * @param result The Hash map where data from the object are written
   */
  protected Status readFromStorage(String tableName, String key, Set<String> fields, Map<String, ByteIterator> result) {
    try {
      FilerProto.Entry entry = this.filerClient.lookupEntry(this.folder + "/" + tableName, key);
      if (entry!=null) {
        readOneEntry(entry, key, fields, result);
      }else{
        System.err.println("Fail to read the object " + key);
        return Status.NOT_FOUND;
      }
    } catch (Exception e) {
      System.err.println("Not possible to get the object " + key);
      e.printStackTrace();
      return Status.ERROR;
    }

    return Status.OK;
  }

  protected void readOneEntry(
          FilerProto.Entry entry, String key, Set<String> fields, Map<String, ByteIterator> result) throws IOException {
    List<SeaweedRead.VisibleInterval> visibleIntervalList =
        SeaweedRead.nonOverlappingVisibleIntervals(entry.getChunksList());
    int length = (int) SeaweedRead.totalSize(entry.getChunksList());
    byte[] buffer = new byte[length];
    SeaweedRead.read(this.filerGrpcClient, visibleIntervalList, 0, buffer, 0, buffer.length);
    fromJson(new String(buffer, StandardCharsets.UTF_8), fields, result);
  }

  /**
   * Perform an emulation of a database scan operation on a SeaweedFS table.
   *
   * @param tableName The name of the table
   * @param startkey  The file key of the first file to read.
   * @param recordcount The number of files to read
   * @param fields    The list of fields to read, or null for all of them
   * @param result    A Vector of HashMaps, where each HashMap is a set field/value
   *          pairs for one file
   */
  protected Status scanFromStorage(String tableName, String startkey,
                   int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {

    try {
      List<FilerProto.Entry> entryList = this.filerClient.listEntries(
              this.folder + "/" + tableName, "", startkey, recordcount, true);
      for (FilerProto.Entry entry : entryList) {
        HashMap<String, ByteIterator> ret = new HashMap<String, ByteIterator>();
        readOneEntry(entry, entry.getName(), fields, ret);
        result.add(ret);
      }
    } catch (Exception e) {
      System.err.println("Not possible to list the object " + startkey + " limit " + recordcount);
      e.printStackTrace();
      return Status.ERROR;
    }

    return Status.OK;
  }

  protected static void fromJson(
          String value, Set<String> fields,
          Map<String, ByteIterator> result) throws IOException {
    JsonNode json = MAPPER.readTree(value);
    boolean checkFields = fields != null && !fields.isEmpty();
    for (Iterator<Map.Entry<String, JsonNode>> jsonFields = json.getFields();
         jsonFields.hasNext();
      /* increment in loop body */) {
      Map.Entry<String, JsonNode> jsonField = jsonFields.next();
      String name = jsonField.getKey();
      if (checkFields && !fields.contains(name)) {
        continue;
      }
      JsonNode jsonValue = jsonField.getValue();
      if (jsonValue != null && !jsonValue.isNull()) {
        result.put(name, new StringByteIterator(jsonValue.asText()));
      }
    }
  }

  protected static String toJson(Map<String, ByteIterator> values)
          throws IOException {
    ObjectNode node = MAPPER.createObjectNode();
    Map<String, String> stringMap = StringByteIterator.getStringMap(values);
    for (Map.Entry<String, String> pair : stringMap.entrySet()) {
      node.put(pair.getKey(), pair.getValue());
    }
    JsonFactory jsonFactory = new JsonFactory();
    Writer writer = new StringWriter();
    JsonGenerator jsonGenerator = jsonFactory.createJsonGenerator(writer);
    MAPPER.writeTree(jsonGenerator, node);
    return writer.toString();
  }

}
