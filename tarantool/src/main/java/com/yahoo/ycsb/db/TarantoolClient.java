package com.yahoo.ycsb.db;

import java.util.Map;
import java.util.Set;
import java.util.List;
import java.util.Vector;
import java.util.HashMap;
import java.util.Properties;
import java.util.logging.Level;

import org.tarantool.core.Tuple;
import org.tarantool.core.TarantoolConnection;
import org.tarantool.core.exception.TarantoolException;
import org.tarantool.core.exception.CommunicationException;
import org.tarantool.core.impl.SocketChannelTarantoolConnection;

import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.StringByteIterator;

public class TarantoolClient extends DB{
	TarantoolConnection connection;
	
	public static final String HOST_PROPERTY  = "tnt.host" ;
	public static final String PORT_PROPERTY  = "tnt.port" ;
	public static final String CALL_PROPERTY  = "tnt.call" ;
	public static final String SPACE_PROPERTY = "tnt.space";
	private boolean callValue;

	private int space = 0;
	public void init() throws CommunicationException {
		Properties props = getProperties();
		int port = 33013;
		String address = props.getProperty(HOST_PROPERTY);
		if (address == null) {
			address = "localhost";
		}
		String portString = props.getProperty(PORT_PROPERTY);
		if (portString != null) {
			port = Integer.parseInt(portString);
		}
		String callString = props.getProperty(CALL_PROPERTY);
		if (callString == null) {
			this.callValue = false;
		} else {
			this.callValue = Boolean.parseBoolean(callString);
		}

		String spaceString = props.getProperty(SPACE_PROPERTY);
		if (spaceString != null) {
			this.space = Integer.parseInt(spaceString);
		}
		try {
			connection = new SocketChannelTarantoolConnection(address, port);
		} catch (CommunicationException e) {
			e.printStackTrace();
			throw e;
		}
	}
	
	public void cleanup(){
		connection.close();
	}

	@Override
	public int insert(String table, String key,
			HashMap<String, ByteIterator> values) {
		int j = 0;

		Tuple tuple = new Tuple(2 * values.size() + (this.callValue ? 2 : 1));
		try {
			if (this.callValue) {
				tuple.setLuaArgument(0, space);
				tuple.setLuaString(1, key, "UTF-8");
				for (Map.Entry<String, ByteIterator> i: values.entrySet()) {
					tuple.setLuaString(2 * j + 2, i.getKey(), "UTF-8");
					tuple.setLuaString(2 * j + 3, i.getValue().toString(), "UTF-8");
					j++;
				}
				connection.call(space, "box.replace", tuple);
			}
			else {
				tuple.setString(0, key, "UTF-8");
				for (Map.Entry<String, ByteIterator> i: values.entrySet()) {
					tuple.setString(2 * j + 1, i.getKey(), "UTF-8");
					tuple.setString(2 * j + 2, i.getValue().toString(), "UTF-8");
					j++;
				}
				connection.insertOrReplace(space, tuple);
			}
		} catch (TarantoolException e) {
			e.printStackTrace();
			return 1;
		}
		return 0;
	}

	private HashMap<String, ByteIterator> tuple_convert_filter (Tuple response, 
								    Set<String> fields) {
		HashMap<String, ByteIterator> result = new HashMap<String, ByteIterator>();
		if (response == null)
			return result;
		for (int i = 1; i < response.size(); i += 2) {
			if (fields == null || fields.contains(response.getString(i, "UTF-8")))
				result.put(response.getString(i, "UTF-8"), 
					   new StringByteIterator(response.getString(i + 1, 
										 "UTF-8")));
		}
		return result;
	}

	@Override
	public int read(String table, String key, Set<String> fields,
			HashMap<String, ByteIterator> result) {
		Tuple tuple = new Tuple(this.callValue ? 3 : 1);
		try {
			Tuple resp;
			if (this.callValue) {
				tuple.setLuaArgument(0, space);
				tuple.setLuaArgument(1, 0);
				tuple.setLuaString(2, key, "UTF-8");
				resp = connection.call(space, "box.select", tuple).get(0);
			} else {
				tuple.setString(0, key, "UTF-8");
				resp = connection.findOne(space, 0, 0, tuple);
				if (resp == null) {
					return 1;
				}
			}
			result = tuple_convert_filter(resp, fields);
			return 0;
		} catch (TarantoolException e) {
			e.printStackTrace();
			return 1;
		} catch (IndexOutOfBoundsException e) {
			return 1;
		}
	}
	@Override
	public int scan(String table, String startkey, int recordcount, Set<String> fields,
			Vector<HashMap<String, ByteIterator>> result) {
		Tuple tuple = new Tuple(4);
		tuple.setLuaArgument(0, space);
		tuple.setLuaArgument(1, 0);
		tuple.setLuaArgument(2, recordcount);
		tuple.setLuaString(3, startkey, "UTF-8");
		List<Tuple> response;
		try {
			response = connection.call(space, "box.select_range", tuple);
		} catch (TarantoolException e) {
			e.printStackTrace();
			return 1;
		}
		for (Tuple i: response) {
			HashMap<String, ByteIterator> temp = tuple_convert_filter(i, fields);
			if (temp.isEmpty())
				continue;
			result.add((HashMap<String, ByteIterator>) temp.clone());
		}
		return 0;
	}
	@Override
	public int delete(String table, String key) {
		Tuple tuple = new Tuple(this.callValue ? 2 : 1);
		try {
			if (this.callValue) {
				tuple.setLuaArgument(0, space);
				tuple.setLuaString(1, key, "UTF-8");
				connection.call(space, "box.delete", tuple).get(0);
			} else {
				tuple.setString(0, key, "UTF-8");
				if (connection.delete(space, tuple) == 0)
					return 1;
			}
		} catch (TarantoolException e) {
			e.printStackTrace();
			return 1;
		} catch (IndexOutOfBoundsException e) {
			return 1;
		}
		return 0;
	} 
	@Override
	public int update(String table, String key,
			HashMap<String, ByteIterator> values) {
		return insert(table, key, values);
	}
}
