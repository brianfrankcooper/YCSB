/*
 * Copyright (c) 2023, Hopsworks AB. All rights reserved.
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

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.nio.reactor.ConnectingIOReactor;
import org.apache.http.nio.reactor.IOReactorException;
import org.apache.http.util.EntityUtils;
import site.ycsb.db.http.MyHttpException;

import java.util.concurrent.Future;

/**
 * MyHttpClientAsync.
 */
public class MyHttpClientAsync extends MyHttpClient {
  private CloseableHttpAsyncClient client;

  public MyHttpClientAsync(int numThreads) throws IOReactorException {
    ConnectingIOReactor ioReactor = new DefaultConnectingIOReactor();
    PoolingNHttpClientConnectionManager cm = new PoolingNHttpClientConnectionManager(ioReactor);
    cm.setMaxTotal(numThreads);
    cm.setDefaultMaxPerRoute(numThreads);
    client = HttpAsyncClients.custom().setConnectionManager(cm).build();
    client.start();
  }

  @Override
  public String execute(HttpRequestBase req) throws MyHttpException {
    HttpResponse resp = null;
    try {
      HttpClientContext context = HttpClientContext.create();
      Future<HttpResponse> future = client.execute(req, context, null);
      resp = future.get();
      if (resp.getStatusLine().getStatusCode() == 200) {
        String b = EntityUtils.toString(resp.getEntity());
        return b;
      }
    } catch (Exception ex) {
      System.out.println(ex.getLocalizedMessage());
    }
    throw new MyHttpException("Req failed code : " + resp.getStatusLine().getStatusCode());
  }
}
