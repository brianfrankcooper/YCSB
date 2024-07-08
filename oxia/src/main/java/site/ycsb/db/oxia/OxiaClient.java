package site.ycsb.db.oxia;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.streamnative.oxia.client.api.AsyncOxiaClient;
import io.streamnative.oxia.client.api.GetOption;
import io.streamnative.oxia.client.api.GetResult;
import io.streamnative.oxia.client.api.OxiaClientBuilder;
import io.streamnative.oxia.client.api.PutOption;
import site.ycsb.*;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ExecutionException;

/**
 * StreamNative Oxia client for YCSB framework.
 */
public final class OxiaClient extends DB {
  private AsyncOxiaClient oxiaClient;


  @Override
  public void init() throws DBException {
    final Properties properties = getProperties();
    final var serviceURL = (String) properties.getOrDefault(CONF_KEY_SERVER_URL,
        CONF_VALUE_DEFAULT_SERVER_URL);
    final var namespace = (String) properties.getOrDefault(CONF_KEY_NAMESPACE,
        CONF_VALUE_DEFAULT_NAMESPACE);
    try {
      this.oxiaClient =
          OxiaClientBuilder.create(serviceURL).namespace(namespace)
              .asyncClient().get();
    } catch (InterruptedException | ExecutionException ex) {
      throw new DBException(ex);
    }
  }

  @Override
  public void cleanup() throws DBException {
    try {
      oxiaClient.close();
    } catch (Exception ex) {
      throw new DBException(ex);
    }
  }

  @Override
  public Status read(String table, String key, Set<String> fields,
                     Map<String, ByteIterator> output) {
    final GetResult result =
        oxiaClient.get(key, Set.of(GetOption.PartitionKey(key))).join();
    if (result == null) {
      return Status.NOT_FOUND;
    }
    deserializeWithFilter(result.getValue(), output, fields);
    return Status.OK;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount,
                     Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  public Status update(String table, String key,
                       Map<String, ByteIterator> values) {
    final GetResult result =
        oxiaClient.get(key, Set.of(GetOption.PartitionKey(key))).join();
    final Map<String, ByteIterator> existingMap = new HashMap<>();
    deserializeWithFilter(result.getValue(), existingMap, null);
    existingMap.putAll(values);
    final byte[] payload = serializeByMap(existingMap);
    try {
      // todo: batch operation
      oxiaClient.put(key, payload,
          Set.of(PutOption.PartitionKey(key))).join();
      return Status.OK;
    } catch (Throwable ex) {
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(String table, String key,
                       Map<String, ByteIterator> values) {
    final byte[] payload = serializeByMap(values);
    try {
      // todo: batch operation
      oxiaClient.put(key, payload,
          Set.of(PutOption.PartitionKey(key))).join();
      return Status.OK;
    } catch (Throwable ex) {
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(String table, String key) {
    if (oxiaClient.delete(key).join()) {
      return Status.OK;
    } else {
      return Status.NOT_FOUND;
    }
  }

  /*
   * The binary serializer for YCSB payloads, the protocol format is as follows.
   * <p>
   * +--------------+-----------+--------------+------------+
   * | field_length |   field   | value_length |    value   |
   * +--------------+-----------+--------------+------------+
   * | (4)Bytes     |  (n)Bytes |  (4)Bytes    |   (n)Bytes |
   * +--------------+-----------+--------------+------------+
   */

  public static byte[] serializeByMap(
      Map<String, ByteIterator> structurePayload) {
    final ByteBuf b = ByteBufAllocator.DEFAULT.heapBuffer();
    try {
      structurePayload.forEach((field, value) -> {
        final byte[] v = value.toArray();
        final byte[] f = field.getBytes(StandardCharsets.UTF_8);
        b.writeInt(f.length);
        b.writeBytes(f);
        b.writeInt(v.length);
        b.writeBytes(v);
      });
      final byte[] payload = new byte[b.readableBytes()];
      b.readBytes(payload);
      return payload;
    } finally {
      b.release();
    }
  }


  public static void deserializeWithFilter(byte[] arrays,
                                           Map<String, ByteIterator> output,
                                           Set<String> containField) {
    final ByteBuf buf = Unpooled.wrappedBuffer(arrays);
    // since oxia has CRC, we don't need check here.
    // plus, it's testing, we can let it failed
    while (buf.isReadable()) {
      final int kl = buf.readInt();
      final byte[] bKey = new byte[kl];
      buf.readBytes(bKey);
      final String field = new String(bKey, StandardCharsets.UTF_8);
      final int vl = buf.readInt();

      if (containField == null || containField.contains(field)) {
        final byte[] bValue = new byte[vl];
        buf.readBytes(bValue);
        final var value = new ByteArrayByteIterator(bValue);
        output.put(field, value);
      } else {
        // get rid of the value
        buf.readerIndex(buf.readerIndex() + vl);
      }
    }
  }

  static final String CONF_KEY_SERVER_URL = "oxia.server";
  static final String CONF_VALUE_DEFAULT_SERVER_URL = "localhost:6648";

  static final String CONF_KEY_NAMESPACE = "oxia.namespace";
  static final String CONF_VALUE_DEFAULT_NAMESPACE = "benchmark";
}
