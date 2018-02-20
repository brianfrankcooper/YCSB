/*
 * Copyright (c) 2015 - 2018 Andreas Bader, 2018 YCSB Contributors All rights reserved.
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

import com.yahoo.ycsb.*;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * h5serv client for YCSB framework.
 * h5serv does not support avg/sum/count
 * h5serv has not append() function or something like that
 * -> an index must be counted to tell h5serv were to put new data
 * h5serv support strings with variable length, but these are buggy -> https://github.com/HDFGroup/h5serv/issues/77
 * -> using a fixed length string
 * h5serv does not yet support hdf5 timestamp datatype
 * -> using long/double
 * h5serv does not support queries with H5T_STD_U64BE or H5T_STD_U64LE due to numexpr (both are unsigned 64bit integers)
 * -> using H5T_STD_I64LE instead (signed 64bit integer)
 */
public class H5ServClient extends TimeseriesDB {
  // internal constants
  private static final String DOMAIN_ENDPOINT = "/";
  private static final String DATATYPES_ENDPOINT = "/datatypes";
  private static final String DATASETS_ENDPOINT = "/datasets";
  private static final String GROUPS_ENDPOINT = "/groups";

  private static final String DATATYPE_NAME = "timeseries";
  private static final String DATASET_NAME = "timeseriesSet";
  private static final String HDF_DOMAIN_BASE = "hdfgroup.org";

  // required properties
  private static final String IP_PROPERTY = "ip";
  private static final String PORT_PROPERTY = "port";

  // optional properties
  private static final String PHASE_PROPERTY = "phase"; // See Client.java
  private static final String METRICNAME_PROPERTY = "metric"; // see CoreWorkload.java
  private static final String METRICNAME_PROPERTY_DEFAULT = "usermetric"; // see CoreWorkload.java
  // FIXME make these obsolete by referring to the TimeSeriesWorkload
  private static final String TAG_PREFIX_PROPERTY = "tagprefix"; // see CoreWorkload.java
  private static final String TAG_PREFIX_PROPERTY_DEFAULT = "TAG"; // see CoreWorkload.java
  private static final String TAG_COUNT_PROPERTY = "tagcount"; // see CoreWorkload.java
  private static final String TAG_COUNT_PROPERTY_DEFAULT = "3"; // see CoreWorkload.java
  private static final String TAG_VALUE_LENGTH_PROPERTY = "tagvaluelength"; // see CoreWorkload.java
  private static final String TAG_VALUE_LENGTH_PROPERTY_DEFAULT = "10"; // see CoreWorkload.java

  private URL urlDatasetValue = null;
  private URL urlGroupLinkDS = null; // URL for Link of Dataset
  private int tagCount;
  private String tagPrefix;
  private String phase;
  private String ip = "localhost";
  private int port = 5000;

  private CloseableHttpClient client;
  private int retries = 3;
  // 0 for unlimited
  private int stringlength = 10;
  private String datatypeId = "";
  private int recordcount;
  private int index = 0;
  private Map<String, String> headers = new HashMap<>();

  /**
   * Initialize any state for this DB.
   * Called once per DB instance; there is one DB instance per client thread.
   */
  @Override
  public void init() throws DBException {
    super.init();
    final Properties properties = getProperties();
    recordcount = Integer.parseInt(properties.getProperty(Client.RECORD_COUNT_PROPERTY, Client.DEFAULT_RECORD_COUNT));
    if (recordcount == 0) {
      recordcount = Integer.MAX_VALUE;
    }
    // FIXME use the property from YCSB.core
    if (!properties.containsKey(PHASE_PROPERTY)) {
      throwMissingProperty(PHASE_PROPERTY);
    }
    if (!properties.containsKey(IP_PROPERTY) && !test) {
      throwMissingProperty(IP_PROPERTY);
    }
    if (!properties.contains(PORT_PROPERTY) && !test) {
      throwMissingProperty(PORT_PROPERTY);
    }
    ip = properties.getProperty(IP_PROPERTY);
    port = Integer.parseInt(properties.getProperty(PORT_PROPERTY));

    String domain = properties.getProperty(METRICNAME_PROPERTY, METRICNAME_PROPERTY_DEFAULT) + "." + HDF_DOMAIN_BASE;
    phase = properties.getProperty(PHASE_PROPERTY, "");
    tagCount = Integer.valueOf(properties.getProperty(TAG_COUNT_PROPERTY, TAG_COUNT_PROPERTY_DEFAULT));
    tagPrefix = properties.getProperty(TAG_PREFIX_PROPERTY, TAG_PREFIX_PROPERTY_DEFAULT);
    stringlength = Integer.valueOf(properties.getProperty(TAG_VALUE_LENGTH_PROPERTY,
        TAG_VALUE_LENGTH_PROPERTY_DEFAULT));
    stringlength = Integer.parseInt(properties.getProperty("stringlength", String.valueOf(stringlength)));
    RequestConfig requestConfig = RequestConfig.custom().build();
    if (!test) {
      try {
        client = HttpClientBuilder.create().setDefaultRequestConfig(requestConfig).build();
      } catch (Exception e) {
        throw new DBException(e);
      }
    }
    if (debug) {
      LOGGER.debug("The following properties are given:");
      for (String element : properties.stringPropertyNames()) {
        LOGGER.debug(element + ": " + properties.getProperty(element));
      }
    }

    URL urlDomain;
    URL urlDatatypes;
    URL urlDatasets;
    try {
      urlDomain = new URL("http", ip, port, DOMAIN_ENDPOINT);
      if (debug) {
        LOGGER.debug("domainURL: {}", urlDomain);
      }
      urlDatatypes = new URL("http", ip, port, DATATYPES_ENDPOINT);
      if (debug) {
        LOGGER.debug("datatypesURL: {}", urlDatatypes);
      }
      urlDatasets = new URL("http", ip, port, DATASETS_ENDPOINT);
      if (debug) {
        LOGGER.debug("datasetsURL: {}", urlDatasets);
      }
      URL urlGroups = new URL("http", ip, port, GROUPS_ENDPOINT);
      if (debug) {
        LOGGER.debug("groupsURL: {}", urlGroups);
      }
    } catch (MalformedURLException e) {
      throw new DBException(e);
    }
    if (debug) {
      LOGGER.info("Creating new Domain in HDF5: {}" + domain);
    }
    headers.put("host", domain);
    if (phase.equals("load")) {
      if (debug) {
        LOGGER.info("Load Phase.");
      }
      // Create Domain
      JSONObject response = runQuery(urlDomain, "", headers, "put");
      if (response.equals(new JSONObject())) {
        LOGGER.warn("runQuery() for domain creation returned null.");
      }
      String id = response.getString("root");
      try {
        urlGroupLinkDS = new URL("http", ip, port, GROUPS_ENDPOINT + "/" + id + "/links/" + "linked_" + DATASET_NAME);
      } catch (MalformedURLException e) {
        LOGGER.error("Failed to build groupLink datasource URL", e);
      }
      // Creating datatype
      response = runQuery(urlDatatypes, createDataTypeQueryObject().toString(), headers, "post");
      if (!response.equals(new JSONObject())) {
        if (response.has("id")) {
          datatypeId = response.getString("id");
        } else {
          LOGGER.warn("Response of datatype creation query has no 'id'.");
        }
      }
      if (debug) {
        LOGGER.info("Datatype ID: {}", datatypeId);
      }
      // Creating dataset
      response = runQuery(urlDatasets, createDataSetQueryObject().toString(), headers, "post");
      if (response.equals(new JSONObject())) {
        if (response.has("id")) {
          if (debug) {
            LOGGER.info("Creating link for linked_{} with ID: {} and URL: {}.",
                DATASET_NAME, response.getString("id"), urlGroupLinkDS);
          }
          try {
            final String endpoint = String.format("%s/%s/value", DATASETS_ENDPOINT, response.getString("id"));
            urlDatasetValue = new URL("http", ip, port, endpoint);
          } catch (MalformedURLException e) {
            LOGGER.error("Response of dataset creation query 'id' could not be transformed into a URL." +
                " Value: '{}'.", response.getString("id"));
          }
          // Creating link for dataset -> avoiding "anonymous dataset" (otherwise querys do not work)
          JSONObject query = new JSONObject();
          query.put("id", response.getString("id"));
          JSONObject response2 = runQuery(urlGroupLinkDS, query.toString(), headers, "put");
          if (response2.equals(new JSONObject())) {
            LOGGER.warn("Response of group link creation for linked_{} query is null.%n", DATASET_NAME);
          }
        } else {
          LOGGER.warn("Response of dataset creation query has no 'id'.");
        }
      }
      if (debug) {
        LOGGER.trace("Dataset Value URL: {}", urlDatasetValue);
      }
    } else {
      if (debug) {
        LOGGER.info("Run Phase.");
      }
      index = recordcount; // also possible to search for the last index in HDF5
      JSONObject datasetDefinitions = runQuery(urlDatasets, "", headers, "get");
      urlDatasetValue = readDatasetValueEndpoint(datasetDefinitions);
    }
  }

  private URL readDatasetValueEndpoint(JSONObject response) {
    if (!response.equals(new JSONObject())) {
      if (!response.has("datasets")) {
        LOGGER.error("Response of dataset get query has no 'datasets'.");
        return null;
      }
      if (response.getJSONArray("datasets").length() <= 0) {
        LOGGER.error("Response has a 'datasets' array with length 0, this should not happen.");
        return null;
      }
      if (response.getJSONArray("datasets").length() != 1) {
        LOGGER.warn("Response has a 'datasets' array with length != 1, using first one.");
      }
      try {
        final String endpoint = String.format("%s/%s/value",
            DATASETS_ENDPOINT, response.getJSONArray("datasets").get(0));
        final URL value = new URL("http", ip, port, endpoint);
        if (debug) {
          LOGGER.info("Dataset Value URL: {}", value);
        }
        return value;
      } catch (MalformedURLException e) {
        LOGGER.error("Query id of dataset could not be transformed into a URL. Value: '{}'.",
            response.getJSONArray("datasets").get(0));
      }
    }
    return null;
  }

  private JSONObject createDataSetQueryObject() {
    JSONObject query = new JSONObject();
    // some space for INSERTs in RUN Phase
    query.put("shape", recordcount * 2);
    query.put("type", datatypeId);
    query.put("maxdims", 0);
    if (debug) {
      LOGGER.info("Creating dataset with the following query: {}", query.toString());
    }
    return query;
  }

  private JSONObject createDataTypeQueryObject() {
    JSONObject query = new JSONObject();
    query.put("name", DATATYPE_NAME);
    JSONObject shape = new JSONObject();
    shape.put("class", "H5S_SCALAR");
    query.put("shape", shape);
    JSONObject type = new JSONObject();
    type.put("class", "H5T_COMPOUND");
    JSONArray fields = new JSONArray();
    // YCSB_KEY
    JSONObject field = new JSONObject();
    field.put("name", "YCSB_KEY");
    JSONObject fieldtype = new JSONObject();
    // should be H5T_STD_U64BE (unsigned 64bit integer) but queries do not work then
    // queries return 500 "internal server error"
    // with "NotImplementedError: variable ``YCSB_KEY`` refers to a 64-bit unsigned integer column,
    // not yet supported in conditions, sorry; please use regular Python selections"
    // from the sever logs
    // H5T_STD_U64LE does also not work, should be even a bit faster than H5T_STD_U64BE,
    // See https://github.com/HDFGroup/h5serv/issues/80
    fieldtype.put("base", "H5T_STD_I64LE");
    fieldtype.put("class", "H5T_INTEGER");
    field.put("type", fieldtype);
    fields.put(field);
    // VALUE
    field = new JSONObject();
    field.put("name", "VALUE");
    fieldtype = new JSONObject();
    fieldtype.put("base", "H5T_IEEE_F64BE");
    fieldtype.put("class", "H5T_FLOAT");
    field.put("type", fieldtype);
    fields.put(field);
    // TAG0 ...TAGn
    for (int i = 0; i < tagCount; i++) {
      field = new JSONObject();
      field.put("name", tagPrefix + i);
      shape = new JSONObject();
      shape.put("class", "H5S_SIMPLE");
      shape.put("dims", new int[]{4});
      field.put("shape", shape);
      fieldtype = new JSONObject();
      fieldtype.put("class", "H5T_STRING");
      fieldtype.put("charSet", "H5T_CSET_ASCII");
      fieldtype.put("strPad", "H5T_STR_NULLTERM");
      fieldtype.put("length", 30);
      field.put("type", fieldtype);
      fields.put(field);
    }
    type.put("fields", fields);
    query.put("type", type);
    if (debug) {
      LOGGER.info("Creating datatype 'timeseries' with the following query: {}", query.toString());
    }
    return query;
  }

  private JSONObject runQuery(URL url, String queryStr, Map<String, String> headers, String queryMethod) {

    try {
      final HttpEntityEnclosingRequestBase request;
      if (queryMethod.equals("") || queryMethod.equalsIgnoreCase("post")) {
        request = new HttpPost(url.toString());
      } else if (queryMethod.equalsIgnoreCase("put")) {
        request = new HttpPut(url.toString());
      } else if (queryMethod.equalsIgnoreCase("get")) {
        request = new HttpGetWithEntity(url.toString());
      } else {
        throw new IllegalArgumentException("Query Method must be set to one of \"\", post, put or get");
      }
      StringEntity requestEntity = new StringEntity(queryStr, ContentType.APPLICATION_JSON);
      request.setEntity(requestEntity);
      HttpResponse response = null;
      if (!headers.isEmpty()) {
        for (Map.Entry<String, String> headerEntry : headers.entrySet()) {
          request.addHeader(headerEntry.getKey(), headerEntry.getValue());
        }
      }
      request.addHeader("accept", "application/json");
      for (int attempt = 0; attempt < retries; attempt++) {
        try {
          response = client.execute(request);
          break;
        } catch (IOException e) {
          // ignore for retrying
        }
      }
      if (response == null) {
        request.releaseConnection();
        LOGGER.error("Connection to {} failed {} times.", url.toString(), retries);
        return new JSONObject();
      }
      if (response.getStatusLine().getStatusCode() == HttpURLConnection.HTTP_OK ||
          response.getStatusLine().getStatusCode() == HttpURLConnection.HTTP_CREATED ||
          response.getStatusLine().getStatusCode() == HttpURLConnection.HTTP_NO_CONTENT ||
          response.getStatusLine().getStatusCode() == HttpURLConnection.HTTP_MOVED_PERM) {
        if (response.getStatusLine().getStatusCode() == HttpURLConnection.HTTP_MOVED_PERM) {
          LOGGER.warn("Query returned HTTP Status 301");
        }
        JSONObject jsonObj = null;
        // Maybe also not HTTP_MOVED_PERM? Can't Test it right now
        if (response.getStatusLine().getStatusCode() != HttpURLConnection.HTTP_NO_CONTENT
            // POST Value does not have a response, but HTTP status 200
            && response.getEntity().getContentLength() != 0) {
          try (BufferedReader bis = new BufferedReader(new InputStreamReader(response.getEntity().getContent()))) {
            jsonObj = new JSONObject(bis.lines().collect(Collectors.joining()));
          }
        }
        EntityUtils.consumeQuietly(response.getEntity());
        request.releaseConnection();
        return jsonObj;
      } else {
        LOGGER.warn("Query returned status code {} with Error: '{}'.",
            response.getStatusLine().getStatusCode(), response.getStatusLine().getReasonPhrase());
        EntityUtils.consumeQuietly(response.getEntity());
        return new JSONObject();
      }
    } catch (Exception e) {
      LOGGER.error("ERROR: Error while trying to query {} for '{}'.", url.toString(), queryStr);
      return new JSONObject();
    }
  }

  /**
   * Cleanup any state for this DB.
   * Called once per DB instance; there is one DB instance per client thread.
   */
  @Override
  public void cleanup() throws DBException {
    try {
      if (!test) {
        client.close();
      }
    } catch (Exception e) {
      throw new DBException(e);
    }
  }

  @Override
  public Status read(String metric, Long timestamp, Map<String, List<String>> tags) {
    if (metric == null || metric.equals("") || timestamp == null) {
      return Status.BAD_REQUEST;
    }
    int counter = 0;
    long timestampLong = timestamp;

    // every part of the query must be url encoded
    // even (,) and & must be encoded correctly
    // both, + or %20 are okay as spaces
    // ?query= must not be encoded
    StringBuilder query = new StringBuilder("(YCSB_KEY == ");
    query.append(timestampLong);
    query.append(")");
    for (Map.Entry<String, List<String>> entry : tags.entrySet()) {
      List<String> arrList = entry.getValue();
      if (arrList.size() > 1) {
        // Avoid double brackets
        query.append(" & (");
      } else {
        query.append(" & ");
      }
      for (String tagValue : arrList) {
        query.append("(" + entry.getKey() + " == '" + tagValue + "') | ");
      }
      int length = query.length();
      query.delete(length - " | ".length(), length);
      if (arrList.size() > 1) {
        // Avoid double brackets
        query.append(")");
      }
    }

    URL queryURL;
    try {
      // ?query= must not be encoded
      queryURL = new URL(urlDatasetValue.getProtocol(), urlDatasetValue.getHost(), urlDatasetValue.getPort(),
          urlDatasetValue.getPath() + "?query=" + URLEncoder.encode(query.toString(), "UTF-8"));
      // The URI variant is probably the better solution (technically),
      // but does not encode as required (e.g., does not encode (,),&)
      // Other solutions can not encode things like " == " directly
      // It must be expected that tag names or values need encoding in a non-default setup
      // so automated encoding is required
      // See http://stackoverflow.com/questions/10786042/java-url-encoding-of-query-string-parameters
      // URI variant (change queryURL to URI) -> does not work, does not encode (,),&,...:
      // //queryURL = new URI(urlDatasetValue.getProtocol(), null, urlDatasetValue.getHost(),
      //    urlDatasetValue.getPort(), urlDatasetValue.getPath(), "query=" + query.toString(), null);
    } catch (UnsupportedEncodingException e) {
      LOGGER.error("Can not encode paramters for read query, Error: [{}].", e.toString());
      return Status.ERROR;
    } catch (MalformedURLException e) {
      LOGGER.error("ERROR: Can not create URL for read query, Error: [{}].", e.toString());
      return Status.ERROR;
    }
    if (debug) {
      LOGGER.error("Read Query String: {}", queryURL.toString());
    }
    if (test) {
      return Status.OK;
    }
    JSONObject response = runQuery(queryURL, "", headers, "get");
    if (debug) {
      LOGGER.info("Response: {}", response);
    }
    if (response.has("value")) {
      JSONArray values = response.getJSONArray("value");
      if (values.length() == 0) {
        return Status.NOT_FOUND;
      }
      for (int i = 0; i < values.length(); i++) {
        if (values.isNull(i)) {
          LOGGER.warn("values at index {} is null.", i);
        } else {
          JSONArray value = values.getJSONArray(0);
          if (value.length() == 0) {
            // is allowed!
            LOGGER.warn("value array has length 0.");
          }
          if (value.isNull(0)) {
            LOGGER.warn("value array index 0 is null.");
          }
          if (value.getLong(0) == timestampLong) {
            counter++;
          }
        }
      }
    } else {
      LOGGER.error("Received response without 'value' for '{}' with headers '{}'.",
          queryURL.toString(), headers);
    }
    if (counter == 0) {
      LOGGER.error("Found no values for metric: {} for timestamp: {}.", metric, timestamp);
      return Status.NOT_FOUND;
    } else if (counter > 1) {
      LOGGER.error("Found more than one value for metric: {} for timestamp: {}.", metric, timestamp);
    }
    return Status.OK;
  }

  @Override
  public Status scan(String metric, Long startTs, Long endTs, Map<String, List<String>> tags,
                     AggregationOperation aggreg, int timeValue, TimeUnit timeUnit) {
    if (metric == null || metric.equals("") || startTs == null || endTs == null) {
      return Status.BAD_REQUEST;
    }
    if (timeValue != 0 && TimeUnit.MILLISECONDS.convert(timeValue, timeUnit) != 1) {
      LOGGER.warn("h5serv does not support granularity, defaulting to one bucket.");
    }
    // every part of the query must be url encoded
    // even (,) and & must be encoded correctly
    // both, + or %20 are okay as spaces
    // ?query= must not be encoded
    StringBuilder query = new StringBuilder("(YCSB_KEY >= ");
    query.append(startTs);
    query.append(") & (YCSB_KEY <= ");
    query.append(endTs);
    query.append(")");
    for (Map.Entry<String, List<String>> entry : tags.entrySet()) {
      List<String> arrList = entry.getValue();
      if (arrList.size() > 1) {
        // Avoid dboule brackets
        query.append(" & (");
      } else {
        query.append(" & ");
      }
      for (String tagValue : arrList) {
        query.append("(" + entry.getKey() + " == '" + tagValue + "') | ");
      }
      int length = query.length();
      query.delete(length - " | ".length(), length);
      if (arrList.size() > 1) {
        // Avoid double brackets
        query.append(")");
      }
    }

    URL queryURL;
    try {
      // ?query= must not be encoded
      queryURL = new URL(urlDatasetValue.getProtocol(), urlDatasetValue.getHost(), urlDatasetValue.getPort(),
          urlDatasetValue.getPath() + "?query=" + URLEncoder.encode(query.toString(), "UTF-8"));
      // The URI variant is probably the better solution (technically) but does not encoded as required
      // (e.g., does not encode (,),&)
      // Other solutions can not encode things like " == " directly
      // It must be expected that tag names or values need encoding in a non-default setup
      // so automated encoding is required
      // See http://stackoverflow.com/questions/10786042/java-url-encoding-of-query-string-parameters
      // URI variant (change queryURL to URI) -> does not work, does not encode (,),&,...:
      // queryURL = new URI(urlDatasetValue.getProtocol(), null, urlDatasetValue.getHost(), urlDatasetValue.getPort(),
      //    urlDatasetValue.getPath(), "query=" + query.toString(), null);
    } catch (UnsupportedEncodingException e) {
      LOGGER.error("Can not encode parameters for scan/avg/sum/count query, Error: '{}'.", e);
      return Status.ERROR;
    } catch (MalformedURLException e) {
      LOGGER.error("Can not create URL for scan/avg/sum/count query, Error: '{}'.", e);
      return Status.ERROR;
    }
    if (debug) {
      LOGGER.debug("Read Query String: {}", queryURL.toString());
    }
    if (test) {
      return Status.OK;
    }
    JSONObject response = runQuery(queryURL, "", headers, "get");
    if (debug) {
      LOGGER.info("Response: {}", response);
    }
    if (response.has("value")) {
      JSONArray jsonArr = response.getJSONArray("value");
      if (jsonArr.length() == 0) {
        // is allowed!
        return Status.NOT_FOUND;
      }
      for (int i = 0; i < jsonArr.length(); i++) {
        if (jsonArr.isNull(i) || jsonArr.getJSONArray(0).length() == 0) {
          LOGGER.error("jsonArr at index {} is null or empty.", i);
        }
      }
    } else {
      LOGGER.error("Received response without 'value' for '{}' with headers '{}'.", queryURL.toString(), headers);
    }
    return Status.OK;
  }

  @Override
  public Status insert(String metric, Long timestamp, double value, Map<String, ByteIterator> tags) {
    if (metric == null || metric.equals("") || timestamp == null) {
      return Status.BAD_REQUEST;
    }
    if (phase.equals("run") && index > recordcount * 2) {
      LOGGER.error("Index is greater than (recordcount*2)-1, which is the maximum amount possible. " +
          "Increase hdf5 shape, lessen INSERTs in RUN Phase or implement PUT Shape for increasing dimension " +
          "(http://h5serv.readthedocs.org/en/latest/DatasetOps/PUT_DatasetShape.html).");
      return Status.BAD_REQUEST;
    }
    try {
      JSONObject query = new JSONObject();
      query.put("start", index);
      query.put("stop", index + 1);
      JSONArray valueArr = new JSONArray();
      valueArr.put(timestamp);
      valueArr.put(value);
      for (Map.Entry entry : tags.entrySet()) {
        valueArr.put(entry.getValue().toString());
      }
      query.put("value", valueArr);
      if (debug) {
        LOGGER.info("Input Query String: {}", query.toString());
      }
      if (test) {
        return Status.OK;
      }
      JSONObject jsonObj = runQuery(urlDatasetValue, query.toString(), headers, "put");
      if (debug) {
        LOGGER.info("jsonArr: ", jsonObj);
      }
      if (jsonObj.equals(new JSONObject())) {
        LOGGER.error("Error in processing insert to metric: {} in {}", metric, urlDatasetValue);
        return Status.ERROR;
      }
      index++;
      return Status.OK;
    } catch (Exception e) {
      LOGGER.error("Error in processing insert to metric: {} in {}: {}", metric, urlDatasetValue, e);
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(String table, String key) {
    return Status.NOT_IMPLEMENTED;
  }
}
