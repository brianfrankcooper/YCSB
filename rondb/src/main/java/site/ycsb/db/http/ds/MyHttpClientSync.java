/*
 * Copyright (c) 2022, Yahoo!, Inc. All rights reserved.
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
 * YCSB binding for <a href="https://rondb.com/">RonDB</a>.
 */

package site.ycsb.db.http.ds;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import site.ycsb.db.http.MyHttpException;

import java.io.IOException;

/**
 * MyHttpClientSync.
 */
public class MyHttpClientSync extends MyHttpClient {

  private ThreadLocal<CloseableHttpClient> httpClients = new ThreadLocal<>();

  public MyHttpClientSync() {
    //cm = new PoolingHttpClientConnectionManager();
    //      cm.setMaxTotal(numThreads);
    //      cm.setDefaultMaxPerRoute(numThreads);
    //HttpHost host = new HttpHost(restServerIP, restServerPort);
    //      cm.setMaxPerRoute(new HttpRoute(host), numThreads);
  }


  public CloseableHttpClient getHttpClient() {
    CloseableHttpClient httpClient;
    httpClient = httpClients.get();
    if (httpClient == null) {
      httpClient = HttpClients.createDefault();
      httpClients.set(httpClient);
      System.out.println("Creating new HttpClient" + httpClient.hashCode());
    }
    return httpClient;
  }

  @Override
  public String execute(HttpRequestBase req) throws MyHttpException {
    CloseableHttpResponse resp = null;
    try {
      resp = getHttpClient().execute(req);
      if (resp.getStatusLine().getStatusCode() == 200) {
        String b = EntityUtils.toString(resp.getEntity());
        return b;
      }
    } catch (Exception e) {
      throw new MyHttpException(e);
    } finally {
      if (resp != null) {
        try {
          resp.close();
        } catch (IOException e) {
          throw new MyHttpException(e);
        }
      }
    }
    throw new MyHttpException("Req failed code : " + resp.getStatusLine().getStatusCode());
  }
}
