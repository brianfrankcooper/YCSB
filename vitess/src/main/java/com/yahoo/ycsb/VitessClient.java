package com.yahoo.ycsb;

import com.google.common.base.Joiner;
import com.google.common.primitives.UnsignedLong;

import com.youtube.vitess.vtgate.BindVariable;
import com.youtube.vitess.vtgate.Exceptions.ConnectionException;
import com.youtube.vitess.vtgate.KeyRange;
import com.youtube.vitess.vtgate.KeyspaceId;
import com.youtube.vitess.vtgate.Query;
import com.youtube.vitess.vtgate.Query.QueryBuilder;
import com.youtube.vitess.vtgate.Row;
import com.youtube.vitess.vtgate.Row.Cell;
import com.youtube.vitess.vtgate.VtGate;
import com.youtube.vitess.vtgate.cursor.Cursor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.Vector;

public class VitessClient extends DB {
  private VtGate vtgate;
  private String keyspace;
  private String tabletType;

  private static final String PRIMARY_KEY_COL = "pri_key";

  @Override
  public void init() throws DBException {
    String hosts = getProperties().getProperty("hosts");
    int timeoutMs = Integer.parseInt(getProperties().getProperty("connectionTimeoutMs", "0"));
    keyspace = getProperties().getProperty("keyspace", "ycsb");
    tabletType = getProperties().getProperty("tabletType", "master");
    String createTable = getProperties().getProperty("createTable", null);
    String dropTable = getProperties().getProperty("dropTable", null);
    try {
      vtgate = VtGate.connect(hosts, timeoutMs);
      vtgate.begin();
      if (dropTable != null) {
        vtgate.execute(
            new QueryBuilder(dropTable, keyspace, "master").addKeyRange(KeyRange.ALL).build());
      }
      if (createTable != null) {
        vtgate.execute(
            new QueryBuilder(createTable, keyspace, "master").addKeyRange(KeyRange.ALL).build());
      }
      vtgate.commit();
    } catch (Exception e) {
      throw new DBException(e);
    }
  }

  @Override
  public int read(String table, String key, Set<String> fields,
      HashMap<String, ByteIterator> result) {
    StringBuilder sql = new StringBuilder();
    sql.append("select ");
    if (fields == null || fields.isEmpty()) {
      sql.append("*");
    } else {
      sql.append(Joiner.on(" ").join(fields));
    }
    sql.append(" from ");
    sql.append(table);
    sql.append(" where pri_key = :pri_key");
    System.out.println(sql);
    Query query = new Query.QueryBuilder(sql.toString(), keyspace, tabletType)
        .addKeyspaceId(KeyspaceId.valueOf(getKeyspaceId(key)))
        .addBindVar(BindVariable.forString(PRIMARY_KEY_COL, key)).build();
    try {
      Cursor cursor = vtgate.execute(query);
      for (Row row : cursor) {
        for (Cell cell : row) {
          result.put(cell.getName(), new ByteArrayByteIterator(row.getBytes(cell.getName())));
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      return 1;
    }
    return 0;
  }

  @Override
  public int scan(String table, String startkey, int recordcount, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {
    return 0;
  }

  @Override
  public int update(String table, String key, HashMap<String, ByteIterator> values) {
    List<String> colNames = new ArrayList<String>(values.keySet());
    List<BindVariable> bindVars = new ArrayList<BindVariable>();
    for (String colName : colNames) {
      bindVars.add(BindVariable.forBytes(colName, values.get(colName).toArray()));
    }
    bindVars.add(BindVariable.forString(PRIMARY_KEY_COL, key));

    StringBuilder sql = new StringBuilder();
    sql.append("update ");
    sql.append(table);
    sql.append(" set ");

    StringBuilder updateCols = null;
    for (String colName : values.keySet()) {
      if (updateCols == null) {
        updateCols = new StringBuilder();
      } else {
        updateCols.append(", ");
      }
      updateCols.append(colName);
      updateCols.append("=");
      updateCols.append(":" + colName);
    }
    if (updateCols != null) {
      sql.append(updateCols.toString());
    }
    sql.append("where pri_key = ':pri_key'");

    Query query = new Query.QueryBuilder(sql.toString(), keyspace, "master")
        .addKeyspaceId(KeyspaceId.valueOf(getKeyspaceId(key))).setBindVars(bindVars).build();
    try {
      vtgate.begin();
      vtgate.execute(query);
      vtgate.commit();
    } catch (Exception e) {
      e.printStackTrace();
      return 1;
    }
    return 0;
  }

  @Override
  public int insert(String table, String key, HashMap<String, ByteIterator> values) {
    List<String> colNames = new ArrayList<String>(values.keySet());
    List<BindVariable> bindVars = new ArrayList<BindVariable>();
    for (String colName : colNames) {
      bindVars.add(BindVariable.forBytes(colName, values.get(colName).toArray()));
    }
    colNames.add(KeyspaceId.COL_NAME);
    colNames.add(PRIMARY_KEY_COL);

    bindVars.add(BindVariable.forULong(KeyspaceId.COL_NAME, getKeyspaceId(key)));
    bindVars.add(BindVariable.forString(PRIMARY_KEY_COL, key));

    StringBuilder sql = new StringBuilder();
    sql.append("insert into ");
    sql.append(table);
    sql.append(" (");
    sql.append(Joiner.on(',').join(colNames));
    sql.append(") values (:");
    sql.append(Joiner.on(", :").join(colNames));
    sql.append(" )");

    System.out.println(sql);
    Query query = new Query.QueryBuilder(sql.toString(), keyspace, "master")
        .addKeyspaceId(KeyspaceId.valueOf(getKeyspaceId(key))).setBindVars(bindVars).build();
    try {
      vtgate.begin();
      vtgate.execute(query);
      vtgate.commit();
    } catch (Exception e) {
      e.printStackTrace();
      return 1;
    }
    return 0;
  }

  @Override
  public int delete(String table, String key) {
    StringBuilder sql = new StringBuilder();
    sql.append("delete from ");
    sql.append(table);
    sql.append(" where pri_key = :pri_key");
    Query query = new Query.QueryBuilder(sql.toString(), keyspace, "master")
        .addKeyspaceId(KeyspaceId.valueOf(getKeyspaceId(key)))
        .addBindVar(BindVariable.forString(PRIMARY_KEY_COL, key)).build();
    try {
      vtgate.begin();
      vtgate.execute(query);
      vtgate.commit();
    } catch (Exception e) {
      e.printStackTrace();
      return 1;
    }
    return 0;
  }

  @Override
  public void cleanup() throws DBException {
    try {
      vtgate.close();
    } catch (ConnectionException e) {
      throw new DBException(e);
    }
  }

  private UnsignedLong getKeyspaceId(String key) {
    int hashCode = key.hashCode();
    return UnsignedLong.valueOf("" + hashCode);
  }
}
