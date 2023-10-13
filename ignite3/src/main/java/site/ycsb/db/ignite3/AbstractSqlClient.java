package site.ycsb.db.ignite3;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import site.ycsb.ByteIterator;

abstract class AbstractSqlClient extends IgniteAbstractClient {

  static String prepareInsertStatement(String table, String key, Map<String, ByteIterator> values) {
    List<String> columns = new ArrayList<>(Collections.singletonList(PRIMARY_COLUMN_NAME));
    List<String> insertValues = new ArrayList<>(Collections.singletonList(key));

    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      columns.add(entry.getKey());
      insertValues.add(entry.getValue().toString());
    }

    String columnsString = String.join(", ", columns);
    String valuesString = insertValues.stream()
        .map(e -> "'" + e.replaceAll("'", "''") + "'")
        .collect(Collectors.joining(", "));

    return String.format("INSERT INTO %s (%s) VALUES (%s)", table, columnsString, valuesString);
  }

  static String prepareReadStatement(String table) {
    return String.format("SELECT * FROM %s WHERE %s = ?", table, PRIMARY_COLUMN_NAME);
  }
}
