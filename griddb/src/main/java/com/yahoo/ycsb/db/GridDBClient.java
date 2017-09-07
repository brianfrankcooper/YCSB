/*
   Copyright (c) 2016 TOSHIBA CORPORATION.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package com.yahoo.ycsb.db;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.Vector;

import com.toshiba.mwcloud.gs.ColumnInfo;
import com.toshiba.mwcloud.gs.Container;
import com.toshiba.mwcloud.gs.ContainerInfo;
import com.toshiba.mwcloud.gs.ContainerType;
import com.toshiba.mwcloud.gs.GSException;
import com.toshiba.mwcloud.gs.GSType;
import com.toshiba.mwcloud.gs.GridStore;
import com.toshiba.mwcloud.gs.GridStoreFactory;
import com.toshiba.mwcloud.gs.PartitionController;
import com.toshiba.mwcloud.gs.Query;
import com.toshiba.mwcloud.gs.Row;
import com.toshiba.mwcloud.gs.RowSet;
import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;


public class GridDBClient extends com.yahoo.ycsb.DB
{
	//(A)multicast method
	public String notificationAddress = ""; // "239.0.0.1"
	public String notificationPort = ""; // "31999"
	//(B)fixed list method
	public String notificationMember = ""; // "10.0.0.12:10001,10.0.0.13:10001,10.0.0.14:10001"
	
	public String userName = ""; //
	public String password = ""; //
	public String clusterName = ""; // "ycsbCluster"


	public static final String VALUE_COLUMN_NAME_PREFIX= "field";
	public static final int ROW_KEY_COLUMN_POS = 0;
	public static final String CONTAINER_PREFIX = "usertable@";

	public static final int DEFAULT_CACHE_CONTAINER_NUM = 1000;
	public static final int FIELD_NUM = 10;
	
	public int numContainer = 0; // Sets PartitionNum
	public GSType SCHEMA_TYPE = GSType.STRING;
	
	public GridStore store;
	public ContainerInfo containerInfo = null;

    class RowComparator implements Comparator<Row> {
        public int compare(Row row1, Row row2) throws NullPointerException {
			int result = 0;
			try {
				Object val1 = row1.getValue(0);
				Object val2 = row2.getValue(0);
				result = ((String)val1).compareTo((String)val2);
			} catch (GSException e) {
				e.printStackTrace();
				throw new NullPointerException();
			}
			return result;
        }
    }

	public void init() throws DBException
	{
		System.out.println("GridDBClient");

		final Properties props = getProperties();
		notificationAddress = props.getProperty("notificationAddress");
		notificationPort = props.getProperty("notificationPort");
		notificationMember = props.getProperty("notificationMember");
		clusterName = props.getProperty("clusterName");
		userName = props.getProperty("userName");
		password = props.getProperty("password");
		String fieldcount = props.getProperty("fieldcount");
		String fieldlength = props.getProperty("fieldlength");
		
		System.out.println("notificationAddress=" + notificationAddress + " notificationPort=" + notificationPort + " notificationMember=" + notificationMember);
		System.out.println("clusterName=" + clusterName + " userName=" + userName + " password=" + password);
		System.out.println("fieldcount=" + fieldcount + " fieldlength=" + fieldlength);

		final Properties gridstoreProp = new Properties();
		if (clusterName == null || userName == null || password == null) {
			System.err.println("[ERROR] clusterName or userName or password argument not specified");
			return;
		}
		if (fieldcount == null || fieldlength == null) {
			System.err.println("[ERROR] fieldcount or fieldlength argument not specified");
			return;			
		} else {
			if (fieldcount.equals(String.valueOf(FIELD_NUM)) == false || fieldlength.equals("100") == false) {
				System.err.println("[ERROR] Invalid argment: fieldcount or fieldlength");
				return;	
			}
		}
		if (notificationAddress != null) {
			if (notificationPort == null) {
				System.err.println("[ERROR] notificationPort argument not specified");
				return;
			}
			//(A)multicast method
			gridstoreProp.setProperty("notificationAddress", notificationAddress);
			gridstoreProp.setProperty("notificationPort", notificationPort);
		} else if (notificationMember != null) {
			//(B)fixed list method
			gridstoreProp.setProperty("notificationMember", notificationMember);
		} else {
			System.err.println("[ERROR] notificationAddress and notificationMember argument not specified");
		}
		gridstoreProp.setProperty("clusterName", clusterName);
		gridstoreProp.setProperty("user", userName);
		gridstoreProp.setProperty("password", password);

		gridstoreProp.setProperty("containerCacheSize", String.valueOf(DEFAULT_CACHE_CONTAINER_NUM));

		List<ColumnInfo> columnInfoList = new ArrayList<ColumnInfo>();
		ColumnInfo keyInfo = new ColumnInfo("key", SCHEMA_TYPE);
		columnInfoList.add(keyInfo);
        for (int i = 0; i < FIELD_NUM; i++) {
			String columnName = String.format(VALUE_COLUMN_NAME_PREFIX + "%d", i);
    		ColumnInfo info = new ColumnInfo(columnName, SCHEMA_TYPE);
    		columnInfoList.add(info);
        }
		containerInfo = new ContainerInfo(null, ContainerType.COLLECTION, columnInfoList, true);

		try {
			GridStoreFactory.getInstance().setProperties(gridstoreProp);
			store = GridStoreFactory.getInstance().getGridStore(gridstoreProp);
			PartitionController controller = store.getPartitionController();
			numContainer = controller.getPartitionCount();

			for(int k = 0; k < numContainer; k++) {
				String name = CONTAINER_PREFIX + k;
				final Container<?, ?> container = store.putContainer(name, containerInfo, false);
			}
		}
		catch (GSException e) {
			e.printStackTrace();
			throw new DBException();
		}
		
		System.out.println("numContainer=" + numContainer + " containerCasheSize=" + String.valueOf(DEFAULT_CACHE_CONTAINER_NUM));
		
	}

	public void cleanup() throws DBException
	{
		try {
			store.close();
			//System.out.println("cleanup()");
		}catch (GSException e) {
			e.printStackTrace();
		}
	}

	public Status read(String table, String key, Set<String> fields, HashMap<String,ByteIterator> result)
	{
		//System.out.println("table=" + table + " key=" + key + " read");
		try {

			Object rowKey = makeRowKey(containerInfo, key);
			String containerKey = makeContainerKey(key);
			//System.out.println("containerKey=" + containerKey + " rowKey=" + rowKey);

			final Container<Object, Row> container = store.getContainer(containerKey);
			if(container == null) {
				System.err.println("[ERROR]getCollection " + containerKey + " in read()");
				return Status.ERROR;
			}

			Row targetRow = container.get(rowKey);
			if (targetRow == null) {
				System.err.println("[ERROR]get(rowKey)" + " in read()");
				return Status.ERROR;
			}

			//
		    for (int i = 1; i < containerInfo.getColumnCount(); i++) {
				result.put(containerInfo.getColumnInfo(i).getName(), new ByteArrayByteIterator(targetRow.getValue(i).toString().getBytes()));
			}

			return Status.OK;

		} catch (GSException e) {
			e.printStackTrace();
			return Status.ERROR;
		}
	}

	public Status scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String,ByteIterator>> result)
	{
		System.err.println("[ERROR]scan() not supported");
		return Status.ERROR;
	}

	public Status update(String table, String key, HashMap<String,ByteIterator> values)
	{
		//System.out.println("table=" + table + " key=" + key + " update");

		try {
			Object rowKey = makeRowKey(containerInfo, key);
			String containerKey = makeContainerKey(key);
			//System.out.println("containerKey=" + containerKey + " rowKey=" + rowKey);

			final Container<Object, Row> container = store.getContainer(containerKey);
			if(container == null) {
				System.err.println("[ERROR]getCollection " + containerKey + " in update()");
				return Status.ERROR;
			}

			Row targetRow = container.get(rowKey);
			if (targetRow == null) {
				System.err.println("[ERROR]get(rowKey)" + " in update()");
				return Status.ERROR;
			}

			//
			int setCount = 0;
			for (int i = 1; i < containerInfo.getColumnCount() && setCount < values.size(); i++) {
				String columnName = containerInfo.getColumnInfo(i).getName();
				ByteIterator byteIterator = values.get(containerInfo.getColumnInfo(i).getName());
				if (byteIterator != null) {
					Object value = makeValue(containerInfo, i, byteIterator);
					targetRow.setValue(i, value);
					setCount++;
				}
			}
			if (setCount != values.size()) {
				System.err.println("Error setCount = " + setCount);
				return Status.ERROR;
			}

			container.put(targetRow);

			return Status.OK;
		} catch (GSException e) {
			e.printStackTrace();
			return Status.ERROR;
		}
	}

	public Status insert(String table, String key, HashMap<String,ByteIterator> values)
	{
		//System.out.println("table=" + table + " key=" + key + " insert");

		try {

			Object rowKey = makeRowKey(containerInfo, key);
			String containerKey = makeContainerKey(key);
			//
			//System.out.println("containerKey=" + containerKey + " rowKey=" + rowKey);

			final Container<Object, Row> container = store.getContainer(containerKey);
			if(container == null) {
				System.err.println("[ERROR]getCollection " + containerKey + " in insert()");
			}

			Row row = container.createRow();

			row.setValue(ROW_KEY_COLUMN_POS, rowKey);

			//
		    for (int i = 1; i < containerInfo.getColumnCount(); i++) {
				ByteIterator byteIterator = values.get(containerInfo.getColumnInfo(i).getName());
				Object value = makeValue(containerInfo, i, byteIterator);
				row.setValue(i, value);
			}

			container.put(row);

		} catch (GSException e) {
			e.printStackTrace();
			return Status.ERROR;
		}

		return Status.OK;
	}

	public Status delete(String table, String key)
	{
		//System.out.println("table=" + table + " key=" + key + " delete");
		try {
			Object rowKey = makeRowKey(containerInfo, key);
			String containerKey = makeContainerKey(key);
			//
			//System.out.println("containerKey=" + containerKey + " rowKey=" + rowKey);

			final Container<Object, Row> container = store.getContainer(containerKey);
			if(container == null) {
				System.err.println("[ERROR]getCollection " + containerKey + " in read()");
				return Status.ERROR;
			}

			boolean isDelete = container.remove(rowKey);
			if (!isDelete) {
				System.err.println("[ERROR]remove(rowKey)" + " in remove()");
				return Status.ERROR;
			}
		}catch (GSException e) {
			e.printStackTrace();
			return Status.ERROR;
		}
		return Status.OK;
	}

	protected String makeContainerKey(String key) {
		String name = CONTAINER_PREFIX + Math.abs(key.hashCode() % numContainer);
		return name;
	}

	protected Object makeRowKey(ContainerInfo containerInfo, String key) {
		return key;
	}

	protected Object makeValue(ContainerInfo containerInfo, int columnId, ByteIterator byteIterator) {
		return byteIterator.toString();
	}

	protected Object makeQueryLiteral(ContainerInfo containerInfo, Object value) {
		return "'" + value.toString() + "'";
	}
}
