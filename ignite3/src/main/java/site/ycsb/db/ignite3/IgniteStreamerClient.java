/**
 * Copyright (c) 2013-2018 YCSB contributors. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License. See accompanying LICENSE file.
 * <p>
 */
package site.ycsb.db.ignite3;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.SubmissionPublisher;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.table.DataStreamerOptions;
import org.apache.ignite.table.Tuple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import site.ycsb.ByteIterator;
import site.ycsb.DBException;
import site.ycsb.Status;

/**
 * Ignite3 Data Streamer client.
 */
public class IgniteStreamerClient extends IgniteAbstractClient {
  /** Logger. */
  private static final Logger LOG = LogManager.getLogger(IgniteStreamerClient.class);

  /**
   * Count the number of times initialized to teardown on the last
   * {@link #cleanup()}.
   */
  private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);

  /** Data streamer page size. */
  protected static final int DATA_STREAMER_PAGE_SIZE = 1000;

  /** Data streamer auto-flush frequency. */
  protected static final int DATA_STREAMER_AUTOFLUSH_FREQUENCY = 5000;

  /** Record view publisher. */
  protected SubmissionPublisher rvPublisher;

  /** Record view data streamer completable future. */
  protected CompletableFuture<Void> rvStreamerFut;

  /** {@inheritDoc} */
  @Override
  public void init() throws DBException {
    super.init();

    INIT_COUNT.incrementAndGet();

    synchronized (IgniteSqlClient.class) {
      if (rvPublisher != null) {
        return;
      }

      DataStreamerOptions dsOptions = DataStreamerOptions.builder()
          .pageSize(DATA_STREAMER_PAGE_SIZE)
          .autoFlushFrequency(DATA_STREAMER_AUTOFLUSH_FREQUENCY)
          .build();
      rvPublisher = new SubmissionPublisher<Tuple>();
      rvStreamerFut = rView.streamData(rvPublisher, dsOptions);
    }
  }

  /** {@inheritDoc} */
  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    try {
      Tuple value = Tuple.create(fieldCount + 1);

      value.set(PRIMARY_COLUMN_NAME, key);

      values.forEach((k, v) -> value.set(k, v.toString()));

      if (table.equals(cacheName)) {
        rvPublisher.submit(value);
      } else {
        throw new UnsupportedOperationException("Unexpected table name: " + table);
      }

      return Status.OK;
    } catch (Exception e) {
      LOG.error(String.format("Error inserting key: %s", key), e);

      return Status.ERROR;
    }
  }

  /** {@inheritDoc} */
  @Override
  public Status read(String table, String key, Set<String> fields,
                     Map<String, ByteIterator> result) {
    return Status.NOT_IMPLEMENTED;
  }

  /** {@inheritDoc} */
  @Override
  public Status update(String table, String key,
                       Map<String, ByteIterator> values) {
    return Status.NOT_IMPLEMENTED;
  }

  /** {@inheritDoc} */
  @Override
  public Status delete(String table, String key) {
    return Status.NOT_IMPLEMENTED;
  }

  /** {@inheritDoc} */
  @Override
  public void cleanup() throws DBException {
    synchronized (IgniteStreamerClient.class) {
      int curInitCount = INIT_COUNT.decrementAndGet();

      if (curInitCount <= 0) {
        try {
          rvPublisher.close();

          rvStreamerFut.join();

          node.close();
          node = null;
        } catch (Exception e) {
          throw new DBException(e);
        }
      }
    }
  }
}
