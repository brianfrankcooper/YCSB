/**
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

package com.yahoo.ycsb.db;


import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.thrift.TException;
import org.hypertable.thrift.SerializedCellsFlag;
import org.hypertable.thrift.SerializedCellsWriter;
import org.hypertable.thrift.ThriftClient;
import org.hypertable.thriftgen.Cell;
import org.hypertable.thriftgen.ClientException;
import org.hypertable.thriftgen.Key;
import org.hypertable.thriftgen.KeyFlag;
import org.hypertable.thriftgen.RowInterval;
import org.hypertable.thriftgen.ScanSpec;
import org.hypertable.thrift.SerializedCellsReader;

import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DBException;

/**
 * Hypertable client for YCSB framework
 */
public class HypertableClient extends com.yahoo.ycsb.DB
{
    private boolean _debug = false;
    
    private ThriftClient connection;
    private long ns;

    private String _columnFamily = "";

    private HashMap<String, String> mFieldMap= new HashMap<String, String>();

    private ScanSpec mSpec = new ScanSpec();

    private RowInterval mRowInterval = new RowInterval();

    private List<String> mColumns = new ArrayList<String>();

    private SerializedCellsReader mReader = new SerializedCellsReader(null);

    private Cell mDeleteCell = new Cell();

    public static final int OK = 0;
    public static final int SERVERERROR = -1;
    
    public static final String NAMESPACE = "/ycsb";
    public static final int THRIFTBROKER_PORT = 38080;

    //TODO: make dynamic
    public static final int BUFFER_SIZE = 4096;
    
    /**
     * Initialize any state for this DB.
     * Called once per DB instance; there is one DB instance per client thread.
     */
    @Override
    public void init() throws DBException
    {        
        if ( (getProperties().getProperty("debug") != null) &&
                (getProperties().getProperty("debug").equals("true")) )
        {
            _debug = true;
        }
        
        try {
            connection = ThriftClient.create("localhost", THRIFTBROKER_PORT);
            
            if (!connection.namespace_exists(NAMESPACE)) {
                connection.namespace_create(NAMESPACE);
            }    
            ns = connection.open_namespace(NAMESPACE);
        } catch (ClientException e) {
            throw new DBException("Could not open namespace", e);
        } catch (TException e) {
            throw new DBException("Could not open namespace", e);
        }
            
        
        _columnFamily = getProperties().getProperty("columnfamily");
        if (_columnFamily == null)
        {
            System.err.println("Error, must specify a " +
            		"columnfamily for Hypertable table");
            throw new DBException("No columnfamily specified");
        }

        List<RowInterval> rowIntervals = new ArrayList<RowInterval>();
        rowIntervals.add(mRowInterval);
        mSpec.setRow_intervals(rowIntervals);
        mSpec.setVersions(1);

        mDeleteCell.key = new Key();
    }

    /**
     * Cleanup any state for this DB.
     * Called once per DB instance; there is one DB instance per client thread.
     */
    @Override
    public void cleanup() throws DBException
    {
        try {
            connection.namespace_close(ns);
        } catch (ClientException e) {
            throw new DBException("Could not close namespace", e);
        } catch (TException e) {
            throw new DBException("Could not close namespace", e);
        }
    }
    
    /**
     * Read a record from the database. Each field/value pair from the result 
     * will be stored in a HashMap.
     *
     * @param table The name of the table
     * @param key The record key of the record to read.
     * @param fields The list of fields to read, or null for all of them
     * @param result A HashMap of field/value pairs for the result
     * @return Zero on success, a non-zero error code on error
     */
    @Override
    public int read(String table, String key, Set<String> fields, 
                    HashMap<String, ByteIterator> result)
    {
        //SELECT _column_family:field[i] 
        //  FROM table WHERE ROW=key MAX_VERSIONS 1;

        if (_debug) {
            System.out.println("Doing read from Hypertable columnfamily " + 
                    _columnFamily);
            System.out.println("Doing read for key: " + key);
        }
        
        try {
            if (null != fields) {

              // Set the start row and end row to key
              mRowInterval.setStart_row(key);
              mRowInterval.setEnd_row(key);

              String qualifiedColumn;
              mColumns.clear();
              for (String field : fields) {
                qualifiedColumn = mFieldMap.get(field);
                if (qualifiedColumn == null) {
                  qualifiedColumn = _columnFamily + ":" + field;
                  mFieldMap.put(field, qualifiedColumn);
                }
                mColumns.add(qualifiedColumn);
              }
              mSpec.setColumns(mColumns);

              mSpec.unsetRow_limit();

              mReader.reset(connection.get_cells_serialized(ns, table, mSpec));
            }
            else {
              mReader.reset(connection.get_row_serialized(ns, table, key));
            }
            while (mReader.next()) {
              result.put(new String(mReader.get_column_qualifier()), 
                         new ByteArrayByteIterator(mReader.get_value()));
            }
        } catch (ClientException e) {
            if (_debug) {
                System.err.println("Error doing read: " + e.message);
            }
            return SERVERERROR;
        } catch (TException e) {
            if (_debug)
                System.err.println("Error doing read");
            return SERVERERROR;
        }

        return OK;
    }
    
    /**
     * Perform a range scan for a set of records in the database. Each 
     * field/value pair from the result will be stored in a HashMap.
     *
     * @param table The name of the table
     * @param startkey The record key of the first record to read.
     * @param recordcount The number of records to read
     * @param fields The list of fields to read, or null for all of them
     * @param result A Vector of HashMaps, where each HashMap is a set 
     *    field/value pairs for one record
     * @return Zero on success, a non-zero error code on error
     */
    @Override
    public int scan(String table, String startkey, int recordcount, 
                    Set<String> fields, 
                    Vector<HashMap<String, ByteIterator>> result)
    {
        //SELECT _columnFamily:fields FROM table WHERE (ROW >= startkey) 
        //    LIMIT recordcount MAX_VERSIONS 1;

        // Set the start row to startkey and clear the end row
        mRowInterval.setStart_row(startkey);
        mRowInterval.setEnd_row(null);

        // Set the column list (null for all columns)
        if (fields == null) {
          mSpec.unsetColumns();
        }
        else {
          String qualifiedColumn;
          mColumns.clear();
          for (String field : fields) {
            qualifiedColumn = mFieldMap.get(field);
            if (qualifiedColumn == null) {
              qualifiedColumn = _columnFamily + ":" + field;
              mFieldMap.put(field, qualifiedColumn);
            }
            mColumns.add(qualifiedColumn);
          }
          mSpec.setColumns(mColumns);
        }

        // Set the LIMIT
        mSpec.setRow_limit(recordcount);

        try {
            long sc = connection.scanner_open(ns, table, mSpec);
                        
            String lastRow = null;
            boolean eos = false;
            while (!eos) {
                mReader.reset(connection.scanner_get_cells_serialized(sc));
                while (mReader.next()) {
                    String currentRow = new String(mReader.get_row());
                    if (!currentRow.equals(lastRow)) {
                        result.add(new HashMap<String, ByteIterator>());
                        lastRow = currentRow;
                    }
                    result.lastElement().put(
                            new String(mReader.get_column_qualifier()), 
                            new ByteArrayByteIterator(mReader.get_value()));
                }
                eos = mReader.eos();

                if (_debug) {
                    System.out.println("Number of rows retrieved so far: " + 
                                        result.size());
                }
            }
            connection.scanner_close(sc);
        } catch (ClientException e) {
            if (_debug) {
                System.err.println("Error doing scan: " + e.message);
            }
            return SERVERERROR;
        } catch (TException e) {
            if (_debug)
                System.err.println("Error doing scan");
            return SERVERERROR;            
        }
        
        return OK;
    }

    /**
     * Update a record in the database. Any field/value pairs in the specified 
     * values HashMap will be written into the record with the specified
     * record key, overwriting any existing values with the same field name.
     *
     * @param table The name of the table
     * @param key The record key of the record to write
     * @param values A HashMap of field/value pairs to update in the record
     * @return Zero on success, a non-zero error code on error
     */
    @Override
    public int update(String table, String key, 
            HashMap<String, ByteIterator> values)
    {
        return insert(table, key, values);
    }

    /**
     * Insert a record in the database. Any field/value pairs in the specified 
     * values HashMap will be written into the record with the specified
     * record key.
     *
     * @param table The name of the table
     * @param key The record key of the record to insert.
     * @param values A HashMap of field/value pairs to insert in the record
     * @return Zero on success, a non-zero error code on error
     */
    @Override
    public int insert(String table, String key, 
            HashMap<String, ByteIterator> values)
    {
        //INSERT INTO table VALUES 
        //  (key, _column_family:entry,getKey(), entry.getValue()), (...);

        if (_debug) {
            System.out.println("Setting up put for key: " + key);
        }
        
        try {
            SerializedCellsWriter writer = 
                    new SerializedCellsWriter(BUFFER_SIZE*values.size(), true);
            for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
                writer.add(key, _columnFamily, entry.getKey(),
                        SerializedCellsFlag.AUTO_ASSIGN,
                        ByteBuffer.wrap(entry.getValue().toArray()));
            }
            connection.set_cells_serialized(ns, table, writer.buffer());
        } catch (ClientException e) {
            if (_debug) {
                System.err.println("Error doing set: " + e.message);
            }
            return SERVERERROR;
        } catch (TException e) {
            if (_debug)
                System.err.println("Error doing set");
            return SERVERERROR;
        }
        
        return OK;
    }

    /**
     * Delete a record from the database.
     *
     * @param table The name of the table
     * @param key The record key of the record to delete.
     * @return Zero on success, a non-zero error code on error
     */
    @Override
    public int delete(String table, String key)
    {
        //DELETE * FROM table WHERE ROW=key;
        
        if (_debug) {
            System.out.println("Doing delete for key: "+key);
        }
        
        mDeleteCell.key.row = key;
        mDeleteCell.key.flag = KeyFlag.DELETE_ROW;

        try {
            connection.set_cell(ns, table, mDeleteCell);
        } catch (ClientException e) {
            if (_debug) {
                System.err.println("Error doing delete: " + e.message);
            }
            return SERVERERROR;            
        } catch (TException e) {
            if (_debug)
                System.err.println("Error doing delete");
            return SERVERERROR;
        }
      
        return OK;
    }
}


