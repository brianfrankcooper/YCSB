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
import org.hypertable.thrift.ThriftClient;
import org.hypertable.thriftgen.Cell;
import org.hypertable.thriftgen.ClientException;
import org.hypertable.thriftgen.Key;
import org.hypertable.thriftgen.KeyFlag;
import org.hypertable.thriftgen.RowInterval;
import org.hypertable.thriftgen.ScanSpec;

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

    public static final int OK = 0;
    public static final int SERVERERROR = -1;
    
    public static final String NAMESPACE = "/ycsb";

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
        
        //TODO: allow dynamic namespace specification?
        try {
            connection = ThriftClient.create("localhost", 38080);
            
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

        List<Cell> values;
        
        //OPT: Use get_cells() call instead of scan?
        
        try {
            if (null != fields) {
                Vector<HashMap<String, ByteIterator>> resMap = 
                        new Vector<HashMap<String, ByteIterator>>();
                if (0 != scan(table, key, 1, fields, resMap)) {
                    return SERVERERROR;
                }
                if (!resMap.isEmpty())
                    result.putAll(resMap.firstElement());
            } else {
                values = connection.get_row(ns, table, key);
                
                for (Cell nextValue : values) {
                    if (_debug) {
                        System.out.println("Result for field: " + 
                                nextValue.key.column_qualifier +
                                " is: " + nextValue.value.toString());
                    }
                    if (0 != nextValue.value.remaining())
                        result.put(nextValue.key.column_qualifier, 
                                byteBufferToByteIterator(nextValue.value));
                }
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
     * Converts ByteBuffer type to ByteArrayByteIterator
     * 
     * @param bb A ByteBuffer
     * @return An equivalent ByteArrayByteIterator
     */
    private ByteArrayByteIterator byteBufferToByteIterator(ByteBuffer bb) {
        return new ByteArrayByteIterator(bb.array(), 
                bb.position(), bb.limit() - bb.position()); 
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
        
        ScanSpec spec = new ScanSpec();
        RowInterval elem = new RowInterval();
        elem.setStart_inclusive(true);
        elem.setStart_row(startkey);
        spec.addToRow_intervals(elem);
        if (null != fields) {
            for (String field : fields) {
                spec.addToColumns(_columnFamily + ":" + field);
            }
        }
        spec.setVersions(1);
        spec.setRow_limit(recordcount);
        
        long sc;
        try {
            sc = connection.scanner_open(ns, table, spec);
            
            int counter = 0;
            String lastRow = null;
            while (counter < recordcount) {
                List<Cell> nextRow = connection.scanner_get_cells(sc);
                if (nextRow.isEmpty())
                    break;
                lastRow = parseCellList(nextRow, result, lastRow);
                counter++;
                if (_debug) {
                    System.out.println("Number of calls made: " + counter);
                    System.out.println("Number of cells retrieved: " + 
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
     * Helper function for scan. Parses a List<Cell> into the right format for 
     * scan's results Vector
     *
     * @param values A list of Cells containing table values to be entered 
     *     into result
     * @param result A Vector of HashMaps, where each HashMap is a set 
     *     field/value pairs for one record
     * @param rowName Contains the name of the row corresponding for the 
     *     pairs in result.lastElement()
     */
    private String parseCellList(List<Cell> values, 
                                 Vector<HashMap<String, ByteIterator>> result, 
                                 String rowName) 
    {
        for (Cell value : values) {
            if (result.isEmpty() || !value.key.getRow().equals(rowName)) {
                result.add(new HashMap<String, ByteIterator>());
                rowName = value.key.getRow();
            }
            result.lastElement().put(value.key.column_qualifier, 
                    byteBufferToByteIterator(value.value));
            if (_debug) {
                System.out.println("Result for field: " + 
                        value.key.column_qualifier +
                        " is: " + value.value.toString());
            }
        }
        return rowName;
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
        //TODO: create the table if none exists?
        
        if (_debug) {
            System.out.println("Setting up put for key: " + key);
        }
        
        //INSERT INTO table VALUES 
        //  (key, _column_family:entry,getKey(), entry.getValue()), (...);

        List<Cell> cells = new ArrayList<Cell>();
        for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
            if (_debug) {
                System.out.println("Adding field/value " + entry.getKey() + 
                        "/" + entry.getValue() + " to put request");
            }
            Cell nextInsert = new Cell();
            nextInsert.key = new Key();
            nextInsert.value = ByteBuffer.wrap(entry.getValue().toArray());
            nextInsert.key.row = key;
            nextInsert.key.column_family = _columnFamily;
            nextInsert.key.column_qualifier = entry.getKey();;
            nextInsert.key.flag = KeyFlag.INSERT;
            cells.add(nextInsert);
        }
        
        try {
            connection.set_cells(ns, table, cells);
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
        
        Cell entry = new Cell();
        entry.key = new Key();
        entry.key.row = key;
        entry.key.flag = KeyFlag.DELETE_ROW;
        entry.key.column_family = _columnFamily;  //necessary?
        
        try {
            connection.set_cell(ns, table, entry);
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


