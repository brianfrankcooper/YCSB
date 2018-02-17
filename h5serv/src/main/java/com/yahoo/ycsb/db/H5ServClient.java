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

/**
 * Created by Andreas Bader on 09.02.16.
 */

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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

// Need to use Json 20140107, newer versions drop an error "Unsupported major.minor version 52.0"

/**
 * h5serv client for YCSB framework.
 * h5serv does not support avg/sum/count
 * h5serv has not append() function or something like that
 * -> an index must be counted to tell h5serv were to put new data
 * h5serv support strings with variable length, but these are buggy -> https://github.com/HDFGroup/h5serv/issues/77
 * -> using a fixed length string
 * h5serv does not yet support hdf5 timestamp datatype
 * -> using long/double
 * h5serv does not support querys with H5T_STD_U64BE or H5T_STD_U64LE due to numexpr (both are unsigned 64bit integers)
 * -> using H5T_STD_I64LE instead (signed 64bit integer)
 */
public class H5ServClient extends TimeseriesDB {

  private static final String PHASE_PROPERTY = "phase"; // See Client.java
  private static final String METRICNAME_PROPERTY = "metric"; // see CoreWorkload.java
  private static final String METRICNAME_PROPERTY_DEFAULT = "usermetric"; // see CoreWorkload.java
  private static final String TAG_PREFIX_PROPERTY = "tagprefix"; // see CoreWorkload.java
  private static final String TAG_PREFIX_PROPERTY_DEFAULT = "TAG"; // see CoreWorkload.java
  private static final String TAG_COUNT_PROPERTY = "tagcount"; // see CoreWorkload.java
  private static final String TAG_COUNT_PROPERTY_DEFAULT = "3"; // see CoreWorkload.java
  private static final String TAG_VALUE_LENGTH_PROPERTY = "tagvaluelength"; // see CoreWorkload.java
  private static final String TAG_VALUE_LENGTH_PROPERTY_DEFAULT = "10"; // see CoreWorkload.java

  private URL urlDomain = null;
  private URL urlDatatypes = null;
  private URL urlDatasets = null;
  private URL urlDatasetValue = null;
  private URL urlGroups = null;
  private URL urlGroupLinkDS = null; // URL for Link of Dataset
  private int tagCount;
  private String tagPrefix;
  private String phase;
  private String ip = "localhost";
  private String domainURL = "/";
  private String datatypesURL = "/datatypes";
  private String datasetsURL = "/datasets";
  private String groupsURL = "/groups";
  private String datatypeName = "timeseries";
  private String datasetName = "timeseriesSet";
  private int port = 5000;
  private String basedomain = "hdfgroup.org";
  private String domain = METRICNAME_PROPERTY_DEFAULT + "." + basedomain;

  private CloseableHttpClient client;
  private int retries = 3;
  // 0 is unlimited
  private int stringlength = 10;
  private String datatypeId = "";
  private int recordcount;
  private int index = 0;
  private List<String[]> headers;

  /**
   * Initialize any state for this DB.
   * Called once per DB instance; there is one DB instance per client thread.
   */
  @Override
  public void init() throws DBException {
    super.init();
    try {
      final Properties properties = getProperties();
      recordcount = Integer.parseInt(properties.getProperty(Client.RECORD_COUNT_PROPERTY, Client.DEFAULT_RECORD_COUNT));
      if (recordcount == 0) {
        recordcount = Integer.MAX_VALUE;
      }
      if (debug) {
        System.out.println("Recordcount: " + recordcount);
      }
      if (!properties.containsKey(PHASE_PROPERTY)) {
        throw new DBException(String.format("No %s given, abort.", PHASE_PROPERTY));
      }
      phase = properties.getProperty(PHASE_PROPERTY, "");
      domain = properties.getProperty(METRICNAME_PROPERTY, METRICNAME_PROPERTY_DEFAULT) + "." + basedomain;
      tagCount = Integer.valueOf(properties.getProperty(TAG_COUNT_PROPERTY, TAG_COUNT_PROPERTY_DEFAULT));
      tagPrefix = properties.getProperty(TAG_PREFIX_PROPERTY, TAG_PREFIX_PROPERTY_DEFAULT);
      stringlength = Integer.valueOf(properties.getProperty(TAG_VALUE_LENGTH_PROPERTY,
          TAG_VALUE_LENGTH_PROPERTY_DEFAULT));
      test = Boolean.parseBoolean(properties.getProperty("test", "false"));
      if (!properties.containsKey("port") && !test) {
        throw new DBException("No port given, abort.");
      }
      port = Integer.parseInt(properties.getProperty("port", String.valueOf(port)));
      stringlength = Integer.parseInt(properties.getProperty("stringlength", String.valueOf(stringlength)));
      if (!properties.containsKey("ip") && !test) {
        throw new DBException("No ip given, abort.");
      }
      ip = properties.getProperty("ip", ip);
      if (debug) {
        System.out.println("The following properties are given: ");
        for (String element : properties.stringPropertyNames()) {
          System.out.println(element + ": " + properties.getProperty(element));
        }
      }
      RequestConfig requestConfig = RequestConfig.custom().build();
      if (!test) {
        client = HttpClientBuilder.create().setDefaultRequestConfig(requestConfig).build();
      }
    } catch (Exception e) {
      throw new DBException(e);
    }

    try {
      urlDomain = new URL("http", ip, port, domainURL);
      if (debug) {
        System.out.println("domainURL: " + urlDomain);
      }
      urlDatatypes = new URL("http", ip, port, datatypesURL);
      if (debug) {
        System.out.println("datatypesURL: " + urlDatatypes);
      }
      urlDatasets = new URL("http", ip, port, datasetsURL);
      if (debug) {
        System.out.println("datasetsURL: " + urlDatasets);
      }
      urlGroups = new URL("http", ip, port, groupsURL);
      if (debug) {
        System.out.println("groupsURL: " + urlGroups);
      }
    } catch (MalformedURLException e) {
      throw new DBException(e);
    }
    if (debug) {
      System.out.println("Creating new Domain in HDF5: " + domain);
    }
    headers = new ArrayList<>();
    headers.add(new String[]{"host", domain});
    if (phase.equals("load")) {
      if (debug) {
        System.out.println("Load Phase.");
      }
      // Creating domain
      JSONObject response = runQuery(urlDomain, "", headers, "put");
      if (response == null) {
        System.out.println("ERROR: runQuery() for domain creation returned null.");
      }
      String id = response.getString("root");
      try {
        urlGroupLinkDS = new URL("http", ip, port, groupsURL + "/" + id + "/links/" + "linked_" + datasetName);
      } catch (MalformedURLException e) {
        e.printStackTrace();
      }
      // Creating datatype
      response = runQuery(urlDatatypes, createDataTypeQueryObject().toString(), headers, "post");
      if (response != null) {
        if (response.has("id")) {
          datatypeId = response.getString("id");
        } else {
          System.out.println("ERROR: Response of datatype creation query has no 'id'.");
        }
      }
      if (debug) {
        System.out.println("Datatype ID: " + datatypeId);
      }
      // Creating dataset
      response = runQuery(urlDatasets, createDataSetQueryObject().toString(), headers, "post");
      if (response != null) {
        if (response.has("id")) {
          if (debug) {
            System.out.printf("Creating link for linked_%s with ID: %s and URL: %s.%n",
                datasetName, response.getString("id"), urlGroupLinkDS);
          }
          try {
            final String endpoint = String.format("%s/%s/value", datasetsURL, response.getString("id"));
            urlDatasetValue = new URL("http", ip, port, endpoint);
          } catch (MalformedURLException e) {
            System.out.printf("ERROR: Response of dataset creation query 'id' could not be transformed into a URL." +
                " Value: '%s'.%n", response.getString("id"));
          }
          // Creating link for dataset -> avoiding "anonymous dataset" (otherwise querys do not work)
          JSONObject query = new JSONObject();
          query.put("id", response.getString("id"));
          JSONObject response2 = runQuery(urlGroupLinkDS, query.toString(), headers, "put");
          if (response2 == null) {
            System.out.printf("ERROR: Response of group link creation for linked_%s query is null.%n", datasetName);
          }
        } else {
          System.out.println("ERROR: Response of dataset creation query has no 'id'.");
        }
      }
      if (debug) {
        System.out.println("Dataset Value URL: " + urlDatasetValue);
      }
    } else {
      if (debug) {
        System.out.println("Run Phase.");
      }
      index = recordcount; // also possible to search for the last index in HDF5
      JSONObject response = runQuery(urlDatasets, "", headers, "get");
      urlDatasetValue = handleUrlDataSetResponse(response);


    }
  }

  private URL handleUrlDataSetResponse(JSONObject response) {
    if (response != null) {
      if (!response.has("datasets")) {
        System.out.println("ERROR: Response of dataset get query has no 'datasets'.");
        return null;
      }
      if (response.getJSONArray("datasets").length() <= 0) {
        System.out.println("ERROR: Response has a 'datasets' array with length 0, this should not happen.");
        return null;
      }
      if (response.getJSONArray("datasets").length() != 1) {
        System.out.println("WARNING: Response has a 'datasets' array with length != 1, using first one.");
      }
      try {
        final String endpoint = String.format("%s/%s/value", datasetsURL, response.getJSONArray("datasets").get(0));
        final URL value = new URL("http", ip, port, endpoint);
        if (debug) {
          System.out.println("Dataset Value URL: " + value);
        }
        return value;
      } catch (MalformedURLException e) {
        System.out.printf("ERROR: Query id of dataset could not be transformed into a URL. Value: '%s'.%n",
            response.getJSONArray("datasets").get(0));
      }
    }
    return null;
  }

  private JSONObject createDataSetQueryObject() {
    JSONObject query = new JSONObject();
    query.put("shape", recordcount * 2); // some space for INSERTs in RUN Phase
    query.put("type", datatypeId);
    query.put("maxdims", 0);
    if (debug) {
      System.out.println("Creating dataset with the following query: " + query.toString());
    }
    return query;
  }

  private JSONObject createDataTypeQueryObject() {
    JSONObject query = new JSONObject();
    query.put("name", datatypeName);
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
      System.out.println("Creating datatype 'timeseries' with the following query: " + query.toString());
    }
    return query;
  }

  private JSONObject runQuery(URL url, String queryStr, List<String[]> headers, String queryMethod) {
    JSONObject jsonObj = new JSONObject();
    HttpResponse response = null;
    try {
      final HttpEntityEnclosingRequestBase method;
      if (queryMethod.equals("") || queryMethod.toLowerCase().equals("post")) {
        method = new HttpPost(url.toString());
      } else if (queryMethod.toLowerCase().equals("put")) {
        method = new HttpPut(url.toString());
      } else if (queryMethod.toLowerCase().equals("get")) {
        method = new HttpGetWithEntity(url.toString());
      } else {
        throw new IllegalArgumentException("Query Method must be set to one of \"\", post, put or get");
      }
      StringEntity requestEntity = new StringEntity(queryStr, ContentType.APPLICATION_JSON);
      method.setEntity(requestEntity);
      if (!headers.isEmpty()) {
        for (String[] strArr : headers) {
          if (strArr.length == 2) {
            method.addHeader(strArr[0], strArr[1]);
          } else {
            System.err.print("ERROR: Array in header list does not have length 2. Header not set.");
          }
        }
      }
      method.addHeader("accept", "application/json");
      int tries = retries + 1;
      while (true) {
        tries--;
        try {
          response = client.execute(method);
          break;
        } catch (IOException e) {
          if (tries < 1) {
            System.err.print("ERROR: Connection to " + url.toString() + " failed " + retries + "times.");
            e.printStackTrace();
            if (response != null) {
              EntityUtils.consumeQuietly(response.getEntity());
            }
            method.releaseConnection();
            return new JSONObject();
          }
        }
      }
      if (response.getStatusLine().getStatusCode() == HttpURLConnection.HTTP_OK ||
          response.getStatusLine().getStatusCode() == HttpURLConnection.HTTP_CREATED ||
          response.getStatusLine().getStatusCode() == HttpURLConnection.HTTP_NO_CONTENT ||
          response.getStatusLine().getStatusCode() == HttpURLConnection.HTTP_MOVED_PERM) {
        if (response.getStatusLine().getStatusCode() == HttpURLConnection.HTTP_MOVED_PERM) {
          System.err.println("WARNING: Query returned HTTP Status 301");
        }
        if (response.getStatusLine().getStatusCode() != HttpURLConnection.HTTP_NO_CONTENT) {
          // Maybe also not HTTP_MOVED_PERM? Can't Test it right now
          if (response.getEntity().getContentLength() != 0) {
            // This if is required, as POST Value does not has an response, only 200 as HTTP Code
            BufferedReader bis = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
            StringBuilder builder = new StringBuilder();
            String line;
            while ((line = bis.readLine()) != null) {
              builder.append(line);
            }
            jsonObj = new JSONObject(builder.toString());
          }

        }
        EntityUtils.consumeQuietly(response.getEntity());
        method.releaseConnection();
      } else {
        System.err.printf("WARNING: Query returned status code %d with Error: '%s'.%n",
            response.getStatusLine().getStatusCode(), response.getStatusLine().getReasonPhrase());
        return new JSONObject();
      }
    } catch (Exception e) {
      System.err.printf("ERROR: Errror while trying to query %s for '%s'.%n", url.toString(), queryStr);
      e.printStackTrace();
      if (response != null) {
        EntityUtils.consumeQuietly(response.getEntity());
      }
      return new JSONObject();
    }
    return jsonObj;
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
        // Avoid dobule brackets
        query.append(" & (");
      } else {
        query.append(" & ");
      }
      for (String tagValue : arrList) {
        query.append("(" + entry.getKey() + " == '" + tagValue + "') | ");
      }
      int length = query.length();
      query.delete(length - 3, length);
      if (arrList.size() > 1) {
        // Avoid dobule brackets
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
      System.err.println("ERROR: Can not encode paramters for read query, Error: '" + e.toString() + "'.");
      return Status.ERROR;
    } catch (MalformedURLException e) {
      System.err.println("ERROR: Can not create URL for read query, Error: '" + e.toString() + "'.");
      return Status.ERROR;
    }
    if (debug) {
      System.out.println("Read Query String: " + queryURL.toString());
    }
    if (test) {
      return Status.OK;
    }
    JSONObject response = null;
    response = runQuery(queryURL, "", headers, "get");
    if (debug) {
      System.err.println("Respone: " + response);
    }
    if (response.has("value")) {
      JSONArray jsonArr = response.getJSONArray("value");
      if (jsonArr.length() == 0) {
        return Status.NOT_FOUND;
      }
      for (int i = 0; i < jsonArr.length(); i++) {
        if (jsonArr.isNull(i)) {
          System.err.println("ERROR: jsonArr Index " + i + " is null.");
        } else {
          JSONArray jsonArr2 = jsonArr.getJSONArray(0);
          if (jsonArr2.length() == 0) {
            // is allowed!
            System.err.println("ERROR: jsonArr2 has length 0.");
          }
          if (jsonArr2.isNull(0)) {
            System.err.println("ERROR: jsonArr2 Index 0 is null.");
          }
          if (jsonArr2.getLong(0) == timestampLong) {
            counter++;
          }
        }
      }
    } else {
      System.err.printf("ERROR: Received response without 'value' for '%s' with headers '%s'.%n",
          queryURL.toString(), headers);
    }
    if (counter == 0) {
      System.err.printf("ERROR: Found no values for metric: %s for timestamp: %d.%n", metric, timestamp);
      return Status.NOT_FOUND;
    } else if (counter > 1) {
      System.err.printf("ERROR: Found more than one value for metric: %s for timestamp: %d.%n", metric, timestamp);
    }
    return Status.OK;
  }

  @Override
  public Status scan(String metric, Long startTs, Long endTs, Map<String, List<String>> tags,
                     AggregationOperation aggreg, int timeValue, TimeUnit timeUnit) {
    if (metric == null || metric.equals("") || startTs == null || endTs == null) {
      return Status.BAD_REQUEST;
    }
    if (timeValue != 0) {
      if (TimeUnit.MILLISECONDS.convert(timeValue, timeUnit) != 1) {
        System.err.println("WARNING: h5serv does not support granularity, defaulting to one bucket.");
      }
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
      query.delete(length - 3, length);
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
      System.err.printf("ERROR: Can not encode parameters for scan/avg/sum/count query, Error: '%s'.%n", e);
      return Status.ERROR;
    } catch (MalformedURLException e) {
      System.err.printf("ERROR: Can not create URL for scan/avg/sum/count query, Error: '%s'.%n", e);
      return Status.ERROR;
    }
    if (debug) {
      System.out.println("Read Query String: " + queryURL.toString());
    }
    if (test) {
      return Status.OK;
    }
    JSONObject response = runQuery(queryURL, "", headers, "get");
    if (debug) {
      System.err.println("Respone: " + response);
    }
    if (response.has("value")) {
      JSONArray jsonArr = response.getJSONArray("value");
      if (jsonArr.length() == 0) {
        // is allowed!
        return Status.NOT_FOUND;
      }
      for (int i = 0; i < jsonArr.length(); i++) {
        if (jsonArr.isNull(i) || jsonArr.getJSONArray(0).length() == 0) {
          System.err.println("ERROR: jsonArr at Index " + i + " is null or has length null.");
        }
      }
    } else {
      System.err.printf("ERROR: Received response without 'value' for '%s' with headers '%s'.%n",
          queryURL.toString(), headers);
    }
    return Status.OK;
  }

  @Override
  public Status insert(String metric, Long timestamp, double value, Map<String, ByteIterator> tags) {
    if (metric == null || metric.equals("") || timestamp == null) {
      return Status.BAD_REQUEST;
    }
    if (phase.equals("run") && index > recordcount * 2) {
      System.err.println("ERROR: index is greater than (recordcount*2)-1, which is the maximum amount possible. " +
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
        System.out.println("Input Query String: " + query.toString());
      }
      if (test) {
        return Status.OK;
      }
      JSONObject jsonObj = runQuery(urlDatasetValue, query.toString(), headers, "put");
      if (debug) {
        System.err.println("jsonArr: " + jsonObj);
      }
      if (jsonObj.equals(new JSONObject())) {
        System.err.println("ERROR: Error in processing insert to metric: " + metric + " in " + urlDatasetValue);
        return Status.ERROR;
      }
      index++;
      return Status.OK;
    } catch (Exception e) {
      System.err.println("ERROR: Error in processing insert to metric: " + metric + " in " + urlDatasetValue + " " + e);
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(String table, String key) {
    return Status.NOT_IMPLEMENTED;
  }
}
