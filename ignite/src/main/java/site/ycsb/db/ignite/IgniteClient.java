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
package site.ycsb.db.ignite;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.binary.BinaryField;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import site.ycsb.ByteIterator;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;

/**
 * Ignite client.
 *
 * See {@code ignite/README.md} for details.
 */
public class IgniteClient extends IgniteAbstractClient {
  /** */
  private static Logger log = LogManager.getLogger(IgniteClient.class);

  static {
    accessMethod = "kv";
  }

  /**
   * Cached binary type.
   */
  private BinaryType binType = null;

  /**
   * Cached binary type's fields.
   */
  private final ConcurrentHashMap<String, BinaryField> fieldsCache = new ConcurrentHashMap<>();

  /** {@inheritDoc} */
  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    try {
      BinaryObject binObj = cache.get(key);

      return convert(binObj, fields, result);
    } catch (Exception e) {
      log.error(String.format("Error reading key: %s", key), e);

      return Status.ERROR;
    }
  }

  /** {@inheritDoc} */
  @Override
  public Status batchRead(String table, List<String> keys, List<Set<String>> fields,
                          List<Map<String, ByteIterator>> results) {
    try {
      Map<String, BinaryObject> map = cache.getAll(new HashSet<>(keys));

      for (int i = 0; i < keys.size(); i++) {
        BinaryObject binObj = map.get(keys.get(i));

        Map<String, ByteIterator> record = new HashMap<>();

        Status status = convert(binObj, fields.get(i), record);

        if (!status.isOk()) {
          return status;
        }

        results.add(record);
      }

      return Status.OK;
    } catch (Exception e) {
      log.error("Error reading batch of keys.", e);

      return Status.ERROR;
    }
  }

  /** {@inheritDoc} */
  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    try {
      cache.invoke(key, new Updater(values));

      return Status.OK;
    } catch (Exception e) {
      log.error(String.format("Error updating key: %s", key), e);

      return Status.ERROR;
    }
  }

  /** {@inheritDoc} */
  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    try {
      BinaryObject binObj = convert(values);

      cache.put(key, binObj);

      return Status.OK;
    } catch (Exception e) {
      log.error(String.format("Error inserting key: %s", key), e);

      return Status.ERROR;
    }
  }

  /** {@inheritDoc} */
  @Override
  public Status batchInsert(String table, List<String> keys, List<Map<String, ByteIterator>> values) {
    try {
      Map<String, BinaryObject> map = new LinkedHashMap<>();

      for (int i = 0; i < keys.size(); i++) {
        BinaryObject binObj = convert(values.get(i));

        map.put(keys.get(i), binObj);
      }

      cache.putAll(map);

      return Status.OK;
    } catch (Exception e) {
      log.error("Error inserting batch of keys.", e);

      return Status.ERROR;
    }
  }

  /** {@inheritDoc} */
  @Override
  public Status delete(String table, String key) {
    try {
      cache.remove(key);

      return Status.OK;
    } catch (Exception e) {
      log.error(String.format("Error deleting key: %s ", key), e);

      return Status.ERROR;
    }
  }

  /**
   * Convert Map<String, ByteIterator> to BinaryObject.
   *
   * @param values Values in Map<String, ByteIterator> format.
   * @return Binary object.
   */
  private BinaryObject convert(Map<String, ByteIterator> values) {
    BinaryObjectBuilder bob = ignite.binary().builder("CustomType");

    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      bob.setField(entry.getKey(), entry.getValue().toString());
    }

    return bob.build();
  }

  /**
   * Convert BinaryObject to Map<String, ByteIterator>.
   *
   * @param binObj Binary object.
   * @param fields Fields set.
   * @param result Result in Map<String, ByteIterator> format.
   * @return Status.
   */
  private Status convert(BinaryObject binObj, Set<String> fields, Map<String, ByteIterator> result) {
    if (binObj == null) {
      return Status.NOT_FOUND;
    }

    if (binType == null) {
      binType = binObj.type();
    }

    for (String s : F.isEmpty(fields) ? binType.fieldNames() : fields) {
      BinaryField binField = fieldsCache.get(s);

      if (binField == null) {
        binField = binType.field(s);
        fieldsCache.put(s, binField);
      }

      String val = binField.value(binObj);
      if (val != null) {
        result.put(s, new StringByteIterator(val));
      }
    }

    return Status.OK;
  }

  /**
   * Entry processor to update values.
   */
  public static class Updater implements CacheEntryProcessor<String, BinaryObject, Object> {
    private String[] flds;
    private String[] vals;

    /**
     * @param values Updated fields.
     */
    Updater(Map<String, ByteIterator> values) {
      flds = new String[values.size()];
      vals = new String[values.size()];

      int idx = 0;
      for (Map.Entry<String, ByteIterator> e : values.entrySet()) {
        flds[idx] = e.getKey();
        vals[idx] = e.getValue().toString();
        ++idx;
      }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object process(MutableEntry<String, BinaryObject> mutableEntry, Object... objects)
        throws EntryProcessorException {
      BinaryObjectBuilder bob = mutableEntry.getValue().toBuilder();

      for (int i = 0; i < flds.length; ++i) {
        bob.setField(flds[i], vals[i]);
      }

      mutableEntry.setValue(bob.build());

      return null;
    }
  }
}
