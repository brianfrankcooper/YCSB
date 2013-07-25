package com.yahoo.ycsb.db;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.logging.Level;

import org.tarantool.core.TarantoolConnection;
import org.tarantool.core.Tuple;
import org.tarantool.core.exception.CommunicationException;
import org.tarantool.core.exception.TarantoolException;
import org.tarantool.core.impl.SocketChannelTarantoolConnection;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.StringByteIterator;

public class TarantoolClient extends DB{
	TarantoolConnection connection;
	
	public static final String HOST_PROPERTY = "tnt.host";
    public static final String PORT_PROPERTY = "tnt.port";
	
    private int space = 0;
    
    public void init() {
		Properties props = getProperties();
        int port = 33013;
		String address = props.getProperty(HOST_PROPERTY);
		if (address == null){
			address = "localhost";
		}
		String portString = props.getProperty(PORT_PROPERTY);
		if (portString != null){
			port = Integer.parseInt(portString);
		}
        try{
        	connection = new SocketChannelTarantoolConnection(address, port);
        }
        catch(CommunicationException e){
        	e.printStackTrace();
        }
	}
	
	public void cleanup(){
		connection.close();
	}

	@Override
	public int insert(String table, String key,
			HashMap<String, ByteIterator> values) {
		int j = 0;
		Tuple tuple = new Tuple(2*values.size() + 1);
		tuple.setString(0, key, "UTF-8");
		for (Map.Entry<String, ByteIterator> i: values.entrySet()){
			tuple.setString(2*j + 1, i.getKey(), "UTF-8").setString(2*j + 2, 
					i.getValue().toString(), "UTF-8");
			j++;
		}
		try{
			connection.insertOrReplace(space, tuple);
		} catch (TarantoolException e){
			e.printStackTrace();
			return 1;
		}
		return 0;
	}

	@Override
	public int read(String table, String key, Set<String> fields,
			HashMap<String, ByteIterator> result){
		Tuple tup = new Tuple(1).setString(0, key, "UTF-8");
		Tuple resp;
		try {
			resp = connection.findOne(space, 0, 0, tup);
		} catch (TarantoolException e) {
			return 1;
		} 
		if (resp == null)
			return 1;
		for (int i = 1; i < resp.size(); i += 2){
			if (fields != null && fields.contains(resp.getString(i, "UTF-8")))
				result.put(resp.getString(i, "UTF-8"), 
						new StringByteIterator(resp.getString(i+1, "UTF-8")));
		}
		return 0;
	}

	@Override
	public int scan(String table, String startkey, int recordcount, Set<String> fields,
			Vector<HashMap<String, ByteIterator>> result) {
		Tuple args = new Tuple(4).setLuaArgument(0, space).setLuaArgument(1, 
				0).setLuaArgument(2, recordcount).setLuaString(3, 
						startkey,"UTF-8");
		List<Tuple> response;
		try {
			response = connection.call(space, "box.select_range", args);
		} catch (TarantoolException e){
			e.printStackTrace();
			return 1;
		}
		HashMap<String, String> temp = new HashMap<String, String>();
		for (Tuple i: response){
			for (int j = 1; j < i.size(); j += 2){
				if (fields != null && fields.contains(i.getString(j, "UTF-8")))
					temp.put(i.getString(j, "UTF-8"), 
							i.getString(j+1, "UTF-8"));
			}
			if (temp.size() != 0)
				result.add((HashMap<String, ByteIterator>) temp.clone());
			temp.clear();
		}
		return 0;
	}

	@Override
	public int delete(String table, String key) {
		try{
			connection.delete(space, new Tuple(1).setString(0, key, "UTF-8"));
		} catch (TarantoolException e){
			e.printStackTrace();
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
