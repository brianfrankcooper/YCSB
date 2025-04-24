/**
 * Copyright (c) 2024 YCSB contributors. All rights reserved.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */
package site.ycsb.db;

import static com.google.cloud.bigtable.data.v2.models.Filters.FILTERS;

import com.google.api.gax.batching.Batcher;
import com.google.api.gax.batching.BatchingException;
import com.google.api.gax.batching.BatchingSettings;
import com.google.api.gax.grpc.ChannelPoolSettings;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.Filters.ChainFilter;
import com.google.cloud.bigtable.data.v2.models.MutationApi;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Range;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import com.google.cloud.bigtable.data.v2.models.TableId;
import com.google.cloud.bigtable.data.v2.stub.EnhancedBigtableStubSettings;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import site.ycsb.ByteIterator;
import site.ycsb.DBException;
import site.ycsb.Status;

/**
 * Idiomatic Java client for Google Bigtable Proto client for YCSB framework.
 */
public class GoogleBigtable2Client extends site.ycsb.DB {

  /**
   * Property names for the CLI.
   */
  private static final String PROP_PREFIX = "googlebigtable2";

  private static final String DEBUG_KEY = "debug";
  private static final String ENDPOINT_KEY = PROP_PREFIX + ".data-endpoint";
  private static final String PROJECT_KEY = PROP_PREFIX + ".project";
  private static final String INSTANCE_KEY = PROP_PREFIX + ".instance";
  private static final String APP_PROFILE_ID_KEY = PROP_PREFIX + ".app-profile";
  private static final String FAMILY_KEY = PROP_PREFIX + ".family";

  private static final String MAX_OUTSTANDING_BYTES_KEY = PROP_PREFIX + ".max-outstanding-bytes";
  private static final String CLIENT_SIDE_BUFFERING_KEY = PROP_PREFIX + ".use-batching";
  private static final String REVERSE_SCANS_KEY = PROP_PREFIX + ".reverse-scans";
  private static final String FIXED_TIMESTAMP_KEY = PROP_PREFIX + ".timestamp";
  // Defaults to autosized
  private static final String CHANNEL_POOL_SIZE = PROP_PREFIX + ".channel-pool-size";

  /**
   * Print debug information to standard out.
   */
  private static boolean debug = false;

  /**
   * Tracks running thread counts so we know when to close the session.
   */
  private static int clientRefCount = 0;

  /**
   * Global Bigtable native API objects.
   */
  private static BigtableDataClient client;

  private static String columnFamily;
  /**
   * If true, buffer mutations on the client. For measuring insert/update/delete latencies, client
   * side buffering should be disabled.
   */
  private static boolean clientSideBuffering = true;
  private static boolean reverseScans = false;
  private static Optional<Long> fixedTimestamp = Optional.empty();

  /**
   * Thread local Bigtable native API objects.
   */
  private final Map<String, Batcher<RowMutationEntry, Void>> batchers = new HashMap<>();

  @Override
  public void init() throws DBException {
    Properties props = getProperties();

    try {
      globalInit(props);
    } catch (Exception e) {
      throw new DBException("Failed to initialize client", e);
    }
  }

  private static synchronized void globalInit(Properties props) throws IOException {
    clientRefCount++;
    if (clientRefCount > 1) {
      return;
    }

    debug = Boolean.parseBoolean(props.getProperty(DEBUG_KEY, "false"));

    // Resource names
    BigtableDataSettings.Builder builder =
        BigtableDataSettings.newBuilder()
            .setProjectId(getRequiredProp(props, PROJECT_KEY))
            .setInstanceId(getRequiredProp(props, INSTANCE_KEY));

    Optional.ofNullable(props.getProperty(APP_PROFILE_ID_KEY)).ifPresent(builder::setAppProfileId);

    Optional.ofNullable(props.getProperty(CHANNEL_POOL_SIZE))
        .map(Integer::parseInt)
        .ifPresent(size ->
            builder.stubSettings().setTransportChannelProvider(
              EnhancedBigtableStubSettings.defaultGrpcTransportProviderBuilder()
                  .setChannelPoolSettings(ChannelPoolSettings.staticallySized(size))
                  .build()
        ));

    columnFamily = getRequiredProp(props, FAMILY_KEY);

    // Endpoint
    Optional<String> emulatorHost =
        Optional.ofNullable(System.getenv().get("BIGTABLE_EMULATOR_HOST"));
    Optional<String> dataEndpoint = Optional.ofNullable(props.getProperty(ENDPOINT_KEY));
    if (emulatorHost.isPresent()) {
      Preconditions.checkState(
          !dataEndpoint.equals(emulatorHost),
          "Can't override endpoint when BIGTABLE_EMULATOR_HOST is set");
    } else {
      dataEndpoint.ifPresent(ep -> builder.stubSettings().setEndpoint(ep));
    }

    // Other settings
    fixedTimestamp = Optional.ofNullable(props.getProperty(FIXED_TIMESTAMP_KEY))
        .map(Long::parseLong);

    clientSideBuffering =
        Optional.ofNullable(props.getProperty(CLIENT_SIDE_BUFFERING_KEY))
            .map(Boolean::parseBoolean)
            .orElse(true);

    reverseScans = Optional.ofNullable(props.getProperty(REVERSE_SCANS_KEY))
        .map(Boolean::parseBoolean)
        .orElse(false);

    Optional.ofNullable(props.getProperty(MAX_OUTSTANDING_BYTES_KEY))
        .map(Long::parseLong)
        .ifPresent(newLimit -> {
            BatchingSettings oldSettings =
                builder.stubSettings().bulkMutateRowsSettings().getBatchingSettings();

            builder
                .stubSettings()
                .bulkMutateRowsSettings()
                .setBatchingSettings(
                    oldSettings.toBuilder()
                        .setFlowControlSettings(
                            oldSettings.getFlowControlSettings().toBuilder()
                                .setMaxOutstandingRequestBytes(newLimit)
                                .build())
                        .build());
          });

    BigtableDataSettings settings = builder.build();

    if (debug) {
      System.out.println("Initializing client with settings: " + settings);
    }

    client = BigtableDataClient.create(settings);
  }

  private static String getRequiredProp(Properties props, String key) {
    String val = props.getProperty(key);
    if (Strings.isNullOrEmpty(val)) {
      throw new IllegalStateException("Missing required property: " + key);
    }
    return val;
  }

  @Override
  public void cleanup() throws DBException {
    for (Batcher<RowMutationEntry, Void> batcher : batchers.values()) {
      try {
        batcher.close();
      } catch (BatchingException e) {
        System.err.println(e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    try {
      globalCleanup();
    } catch (Exception e) {
      throw new DBException("Failed to clean up the shared client", e);
    }
  }

  private static synchronized void globalCleanup() {
    clientRefCount--;
    if (clientRefCount == 0) {
      client.close();
    }
  }

  @Override
  public Status read(
      String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    if (debug) {
      System.out.println("Doing read for key: " + key);
    }

    Filters.Filter filter = buildFilter(fields);

    try {
      Row row = client.readRow(TableId.of(table), key, filter);
      if (debug) {
        System.out.println("Result row: " + row);
      }

      if (row == null) {
        return Status.NOT_FOUND;
      }

      rowToMap(row, result);
      return Status.OK;
    } catch (Exception e) {
      if (debug) {
        e.printStackTrace();
      }
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(
      String table,
      String startkey,
      int recordcount,
      Set<String> fields,
      Vector<HashMap<String, ByteIterator>> results) {
    if (debug) {
      System.out.printf("Doing scan for %s for %d records%n", startkey, recordcount);
    }
    Filters.Filter filter = buildFilter(fields);

    ByteStringRange range = reverseScans
        ? Range.ByteStringRange.unbounded().endClosed(startkey)
        : Range.ByteStringRange.unbounded().startClosed(startkey);

    Query query =
        Query.create(table)
            .reversed(reverseScans)
            .filter(filter)
            .range(range)
            .limit(recordcount);

    final List<Row> rows;
    try {
      rows = client.readRowsCallable().all().call(query);
    } catch (Exception e) {
      if (debug) {
        e.printStackTrace();
      }
      return Status.ERROR;
    }

    for (Row row : rows) {
      HashMap<String, ByteIterator> rowResult = new HashMap<>();
      rowToMap(row, rowResult);
      results.add(rowResult);
    }
    return Status.OK;
  }

  private static void rowToMap(Row row, Map<String, ByteIterator> result) {
    for (RowCell s : row.getCells()) {
      result.put(s.getQualifier().toStringUtf8(), new ByteStringWrapper(s.getValue()));
    }
  }

  @Override
  public Status delete(String table, String key) {
    if (debug) {
      System.out.println("Doing delete for key: " + key);
    }

    if (clientSideBuffering) {
      getBatcher(table).add(RowMutationEntry.create(key).deleteRow());
      return Status.BATCHED_OK;
    }

    RowMutation m = RowMutation.create(TableId.of(table), key).deleteRow();
    try {
      client.mutateRow(m);
    } catch (Exception e) {
      if (debug) {
        e.printStackTrace();
      }
      return Status.ERROR;
    }

    return Status.OK;
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    return update(table, key, values);
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    if (debug) {
      System.out.println("Setting up write for key: " + key);
    }

    try {
      if (clientSideBuffering) {
        return writeBatched(table, key, values);
      } else {
        return writeDirect(table, key, values);
      }
    } catch (Exception e) {
      if (debug) {
        e.printStackTrace();
      }
      return Status.ERROR;
    }
  }

  private Status writeDirect(String table, String key, Map<String, ByteIterator> values) {
    RowMutation m = RowMutation.create(TableId.of(table), key);
    mapToMutation(values, m);

    try {
      client.mutateRow(m);
    } catch (Exception e) {
      if (debug) {
        e.printStackTrace();
      }
      return Status.ERROR;
    }
    return Status.OK;
  }

  private Status writeBatched(String table, String key, Map<String, ByteIterator> values) {
    RowMutationEntry m = RowMutationEntry.create(key);
    mapToMutation(values, m);
    getBatcher(table).add(m);

    return Status.BATCHED_OK;
  }

  private Batcher<RowMutationEntry, Void> getBatcher(String table) {
    return batchers.computeIfAbsent(table, (t) -> client.newBulkMutationBatcher(TableId.of(table)));
  }

  private void mapToMutation(Map<String, ByteIterator> values, MutationApi<?> m) {
    for (Entry<String, ByteIterator> e : values.entrySet()) {
      if (fixedTimestamp.isPresent()) {
        m.setCell(
            columnFamily,
            ByteString.copyFromUtf8(e.getKey()),
            fixedTimestamp.get(),
            ByteString.copyFrom(e.getValue().toArray()));
      } else {
        m.setCell(
            columnFamily,
            ByteString.copyFromUtf8(e.getKey()),
            ByteString.copyFrom(e.getValue().toArray()));
      }
    }
  }

  private static Filters.Filter buildFilter(Set<String> fields) {
    ChainFilter chain = FILTERS.chain()
        .filter(FILTERS.family().exactMatch(columnFamily))
        .filter(FILTERS.limit().cellsPerColumn(1));

    if (fields != null && !fields.isEmpty()) {
      chain = chain.filter(FILTERS.qualifier().exactMatch(String.join("|", fields)));
    }
    return chain;
  }

  private static class ByteStringWrapper extends ByteIterator {

    private final ByteString.ByteIterator it;
    private int remaining;

    public ByteStringWrapper(ByteString value) {
      it = value.iterator();
      remaining = value.size();
    }

    @Override
    public boolean hasNext() {
      return it.hasNext();
    }

    @Override
    public byte nextByte() {
      remaining--;
      return it.nextByte();
    }

    @Override
    public long bytesLeft() {
      return remaining;
    }
  }
}
