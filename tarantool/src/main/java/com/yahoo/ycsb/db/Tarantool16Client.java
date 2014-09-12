package com.yahoo.ycsb.db;

import java.util.Map;
import java.util.Set;
import java.util.List;
import java.util.Arrays;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.HashMap;
import java.util.Properties;

import org.tarantool.msgpack.TarantoolConnection16Impl;
import org.tarantool.core.exception.TarantoolException;
import org.tarantool.core.exception.CommunicationException;

import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.StringByteIterator;

public class Tarantool16Client extends DB{
	TarantoolConnection16Impl connection;
	
	private static final String HOST_PROPERTY       = "tnt.host";
	private static final String PORT_PROPERTY       = "tnt.port";
	private static final String CALL_PROPERTY       = "tnt.call";
	private static final String SPACE_NO_PROPERTY   = "tnt.space.no";
	private static final String SPACE_NAME_PROPERTY = "tnt.space.name";
	private static final String USER_NAME_PROPERTY  = "tnt.user.name";
	private static final String USER_PASS_PROPERTY  = "tnt.user.pass";

	private static final String DEFAULT_HOST        = "localhost";
	private static final String DEFAULT_PORT        = "3301";
	private static final String DEFAULT_CALL        = "false";
	private static final String DEFAULT_SPACE_NO    = "1024";
	private static final String DEFAULT_SPACE_NAME  = "ycsb";
	private static final String DEFAULT_USER_NAME   = "";
	private static final String DEFAULT_USER_PASS   = "";

	private boolean call;
	private String  spaceName;
	private int     spaceNo;
	private int     port;
	private String  host;
	private String  user;
	private String  passwd;
	
	private static final Logger log = Logger.getLogger(Tarantool16Client.class.getName());

	public void init() throws CommunicationException {

		Properties props = getProperties();
		this.host      = props.getProperty(HOST_PROPERTY, DEFAULT_HOST);
		this.port      = Integer.parseInt(props.getProperty(PORT_PROPERTY, DEFAULT_PORT));
		this.spaceNo   = Integer.parseInt(props.getProperty(SPACE_NO_PROPERTY, DEFAULT_SPACE_NO));
		this.spaceName = props.getProperty(SPACE_NAME_PROPERTY, DEFAULT_SPACE_NAME);
		this.user      = props.getProperty(USER_NAME_PROPERTY, DEFAULT_USER_NAME);
		this.passwd    = props.getProperty(USER_PASS_PROPERTY, DEFAULT_USER_PASS);
		this.call      = Boolean.parseBoolean(props.getProperty(CALL_PROPERTY, DEFAULT_CALL));
		System.err.println(this.host + " " + this.port + " " + this.spaceNo + " " +
				this.spaceName + " " + this.user + " " +  this.passwd + " " + this.call);

		try {
			this.connection = new TarantoolConnection16Impl(this.host, this.port);
			if (this.user != "")
				this.connection.auth(this.user, this.passwd);
		} catch (CommunicationException e) {
			e.printStackTrace();
			throw e;
		}
	}
	
	public void cleanup() throws DBException{
		this.connection.close();
	}

	@Override
	public int insert(String table, String key, HashMap<String, ByteIterator> values) {
		int j = 0;
		String[] tuple = new String[1 + 2 * values.size()];
		tuple[0] = key;
		for (Map.Entry<String, ByteIterator> i: values.entrySet()) {
			tuple[j + 1] = i.getKey();
			tuple[j + 2] = i.getValue().toString();
			j += 2;
		}
		try {
			if (this.call) {
				this.connection.call("box.space."+this.spaceName+":replace", tuple);
			} else {
				this.connection.replace(this.spaceNo, tuple);
			}
		} catch (TarantoolException e) {
			e.printStackTrace();
			return 1;
		}
		return 0;
	}

	private HashMap<String, ByteIterator> tuple_convert_filter (String[] input,
			Set<String> fields) {
		HashMap<String, ByteIterator> result = new HashMap<String, ByteIterator>();
		if (input == null)
			return result;
		for (int i = 1; i < input.length; i += 2)
			if (fields == null || fields.contains(input[i]))
				result.put(input[i], new StringByteIterator(input[i+1]));
		return result;
	}

	@Override
	public int read(String table, String key, Set<String> fields,
			HashMap<String, ByteIterator> result) {
		try {
			String[][] response;
			if (this.call) {
				response = this.connection.call(String[][].class,
						"box.space."+this.spaceName+":select",
						Arrays.asList(key));
			} else {
				response = this.connection.select(String[][].class, this.spaceNo, 0, Arrays.asList(key), 0, 1, 0);
			}
			result = tuple_convert_filter(response[0], fields);
			return 0;
		} catch (TarantoolException e) {
			e.printStackTrace();
			return 1;
		} catch (IndexOutOfBoundsException e) {
			return 1;
		}
	}

	@Override
	public int scan(String table, String startkey,
			int recordcount, Set<String> fields,
			Vector<HashMap<String, ByteIterator>> result) {
		String[][] response;
		try {
			response = this.connection.call(String[][].class, "select_range",
					this.spaceNo, 0,
					Arrays.asList(startkey), recordcount);
		} catch (TarantoolException e) {
			e.printStackTrace();
			return 1;
		}
		for (String[] i: response) {
			HashMap<String, ByteIterator> temp = tuple_convert_filter(i, fields);
			if (temp.isEmpty())
				continue;
			result.add((HashMap<String, ByteIterator>) temp.clone());
		}
		return 0;
	}

	@Override
	public int delete(String table, String key) {
		try {
			if (this.call) {
				this.connection.call(String[][].class, "box.space."+this.spaceName+":delete", key);
			} else {
				this.connection.delete(this.spaceNo, Arrays.asList(key));
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
		int j = 0;
		String[] tuple = new String[1 + 2 * values.size()];
		tuple[0] = key;
		for (Map.Entry<String, ByteIterator> i: values.entrySet()) {
			tuple[j + 1] = i.getKey();
			tuple[j + 2] = i.getValue().toString();
			j += 2;
		}
		try {
			if (this.call) {
				this.connection.call(
						"box.space."+this.spaceName+":replace",
						tuple);
			} else {
				this.connection.replace(this.spaceNo, tuple);
			}
		} catch (TarantoolException e) {
			e.printStackTrace();
			return 1;
		}
		return 0;

	}
}
