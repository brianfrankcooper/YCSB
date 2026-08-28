package site.ycsb;

import org.mapdb.*;
import org.mapdb.DB;

import java.util.*;

/**
 * MapDB client for YCSB framework.
 *
 */
public class MapDBClient extends site.ycsb.DB {

  private DB mapDB;
  private HTreeMap<String, TreeMap<String, HashMap<String, byte[]>>> db;

  @Override
  public void init() throws DBException {
    this.mapDB = DBMaker
        .fileDB("file.db")
        .closeOnJvmShutdown()
        .make();

    db = mapDB.hashMap("db")
        .keySerializer(Serializer.STRING)
        .valueSerializer(Serializer.JAVA)
        .createOrOpen();
  }

  public void cleanup() throws DBException {
    db.clear();
    db.close();
    mapDB.close();
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    Map<String, HashMap<String, byte[]>> collection = db.getOrDefault(table, new TreeMap<>());

    Map<String, byte[]> map = collection.get(key);

    if (map == null) {
      return Status.NOT_FOUND;
    }

    for (String field : fields) {
      result.put(field, new ByteArrayByteIterator(map.get(field)));
    }

    return Status.OK;
  }

  @Override
  public Status scan(String table,
                     String startkey,
                     int recordcount,
                     Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    TreeMap<String, HashMap<String, byte[]>> collection = db.getOrDefault(table, new TreeMap<>());
    SortedMap<String, HashMap<String, byte[]>> map = collection.tailMap(startkey);

    int counter = 0;
    for (Map.Entry<String, HashMap<String, byte[]>> entry : map.entrySet()) {
      HashMap<String, ByteIterator> filteredMap = new HashMap<>();
      if (fields == null) {
        for (Map.Entry<String, byte[]> entry1 : entry.getValue().entrySet()) {
          filteredMap.put(entry1.getKey(), new ByteArrayByteIterator(entry1.getValue()));
        }
      } else {
        for (Map.Entry<String, byte[]> entry1 : entry.getValue().entrySet()) {
          if (fields.contains(entry1.getKey())) {
            filteredMap.put(entry1.getKey(), new ByteArrayByteIterator(entry1.getValue()));
          }
        }
      }
      result.add(filteredMap);

      counter += 1;
      if (counter == recordcount) {
        break;
      }
    }

    return Status.OK;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    TreeMap<String, HashMap<String, byte[]>> collection = db.getOrDefault(table, new TreeMap<>());

    HashMap<String, byte[]> map = collection.get(key);

    if (map == null) {
      return Status.NOT_FOUND;
    }

    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      map.put(entry.getKey(), entry.getValue().toArray());
    }
    collection.put(key, map);
    db.put(table, collection);

    return Status.OK;
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    TreeMap<String, HashMap<String, byte[]>> collection = db.getOrDefault(table, new TreeMap<>());

    HashMap<String, byte[]> map = collection.getOrDefault(key, new HashMap<>());

    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      map.put(entry.getKey(), entry.getValue().toArray());
    }
    collection.put(key, map);
    db.put(table, collection);

    return Status.OK;
  }

  @Override
  public Status delete(String table, String key) {
    Map<String, HashMap<String, byte[]>> collection = db.getOrDefault(table, new TreeMap<>());

    Map<String, byte[]> removed = collection.remove(key);

    if (removed == null) {
      return Status.NOT_FOUND;
    }
    return Status.OK;
  }
}