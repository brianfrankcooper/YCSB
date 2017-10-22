/**
 * Copyright (c) 2017 YCSB contributors. All rights reserved.
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
 */

package com.yahoo.ycsb.db;

import com.google.gson.*;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.*;

import static java.io.File.separatorChar;

/**
 * This "database" creates a file on the given path from properties for each insert call.
 * Is should be used to ensure that the data is the same on multiple benchmark runs with different databases.
 */
public class FileStoreClient extends DB {

  /**
   * The name and default value of the property for the output directory for the files.
   */
  public static final String OUTPUT_DIRECTORY_PROPERTY = "outputDirectory";
  public static final String OUTPUT_DIRECTORY_DEFAULT = System.getProperty("user.dir")
      + separatorChar
      + "benchmarkingData"
      + separatorChar;

  /**
   * The property name to enable pretty printing of the json in created files.
   * This will increase the size of the files substantially!
   */
  public static final String ENABLE_PRETTY_PRINTING = "enablePrettyPrinting";
  public static final String ENABLE_PRETTY_PRINTING_DEFAULT = "false";

  private final GsonBuilder gsonBuilder = new GsonBuilder().registerTypeAdapter(ByteIterator.class, new
      ByteIteratorAdapter());
  private Gson gson;
  private final Type valuesType = new TypeToken<Map<String, ByteIterator>>() {}.getType();

  private String outputDirectory;

  @Override
  public void init() throws DBException {
    Properties properties = getProperties();
    outputDirectory = properties.getProperty(OUTPUT_DIRECTORY_PROPERTY, OUTPUT_DIRECTORY_DEFAULT);

    if (outputDirectory.charAt(outputDirectory.length() - 1) != separatorChar) {
      outputDirectory += separatorChar;
    }

    File directory = new File(outputDirectory);

    if (!directory.exists() && !directory.mkdirs()) {
      throw new DBException("Could not create output directory for files with path: " + outputDirectory);
    }

    if (!properties.getProperty(ENABLE_PRETTY_PRINTING, ENABLE_PRETTY_PRINTING_DEFAULT).equals("false")) {
      gsonBuilder.setPrettyPrinting();
    }

    gson = gsonBuilder.create();
  }

  /**
   * Reads the file with the name {@code table}_{@code key}.json.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to read.
   * @param fields The list of fields to read, or null for all of them
   * @param result A HashMap of field/value pairs for the result
   * @return The specified fields in that file, if present.
   * If there is no such file, {@code Status.NOT_FOUND} will be returned.
   */
  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    String filename = getDatabaseFileName(table, key);

    try (JsonReader jsonReader = new JsonReader(new FileReader(filename))) {
      Map<String, ByteIterator> values = gson.fromJson(jsonReader, valuesType);

      for (String field : fields) {
        result.put(field, values.get(field));
      }

      return Status.OK;
    } catch (IOException e) {
      e.printStackTrace();
    }

    return Status.NOT_FOUND;
  }

  @Override
  public Status scan(String table,
                     String startkey,
                     int recordcount,
                     Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    String filename = getDatabaseFileName(table, startkey);

    try (JsonReader jsonReader = new JsonReader(new FileReader(filename))) {
      Map<String, ByteIterator> values = gson.fromJson(jsonReader, valuesType);
      result.add(convertToHashMap(values, fields));

      return Status.OK;
    } catch (IOException e) {
      e.printStackTrace();
    }

    return Status.ERROR;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    String filename = getDatabaseFileName(table, key);

    try (JsonReader jsonReader = new JsonReader(new FileReader(filename))) {
      Map<String, ByteIterator> map = gson.fromJson(jsonReader, valuesType);

      for (String valuesKey : values.keySet()) {
        map.put(valuesKey, values.get(valuesKey));
      }

      insert(table, key, map);

      return Status.OK;
    } catch (IOException e) {
      e.printStackTrace();
    }

    return Status.ERROR;
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    String filename = getDatabaseFileName(table, key);
    String output = gson.toJson(values, valuesType);

    try (FileWriter fileWriter = new FileWriter(filename)) {
      fileWriter.write(output);
      return Status.OK;
    } catch (IOException e) {
      e.printStackTrace();
    }

    return Status.ERROR;
  }

  @Override
  public Status delete(String table, String key) {
    String filename = getDatabaseFileName(table, key);

    if (new File(filename).delete()) {
      return Status.OK;
    }

    return Status.ERROR;
  }

  private <V> HashMap<String, V> convertToHashMap(Map<String, V> map, Set<String> fields) {
    HashMap<String, V> result = new HashMap<>();

    if (fields != null && fields.size() > 0) {
      for (String field : fields) {
        result.put(field, map.get(field));
      }
    } else {
      result.putAll(map);
    }

    return result;
  }

  private String getDatabaseFileName(String table, String key) {
    return outputDirectory + table + "_" + key + ".json";
  }

  private class ByteIteratorAdapter implements JsonSerializer<ByteIterator>, JsonDeserializer<ByteIterator> {

    private final String typeIdentifier = "type";
    private final String propertyIdentifier = "properties";

    @Override
    public JsonElement serialize(ByteIterator byteIterator,
                                 Type type,
                                 JsonSerializationContext jsonSerializationContext) {
      JsonObject result = new JsonObject();
      result.add(typeIdentifier, new JsonPrimitive(byteIterator.getClass().getName()));
      result.add(propertyIdentifier, jsonSerializationContext.serialize(byteIterator));

      return result;
    }

    @Override
    public ByteIterator deserialize(JsonElement jsonElement,
                                    Type type,
                                    JsonDeserializationContext jsonDeserializationContext) throws JsonParseException {
      JsonObject jsonObject = jsonElement.getAsJsonObject();
      String typeString = jsonObject.get(typeIdentifier).getAsString();
      JsonElement element = jsonObject.get(propertyIdentifier);

      try {
        return jsonDeserializationContext.deserialize(element, Class.forName(typeString));
      } catch (ClassNotFoundException e) {
        throw new JsonParseException("Could not find class " + typeString, e);
      }
    }
  }
}
