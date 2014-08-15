package com.yahoo.ycsb.db;


import com.google.common.base.Preconditions;
import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.measurements.Measurements;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

/**
 * Written for HBase 0.98.5-hadoop2
 *
 * Create a Table in the shell like:
 *
 * CREATE 'tablename', 'columnfamilyname'
 *
 * set autoflush to true for immediate flushes
 *
 *
 * use for example as follows:
 *
 * ./bin/ycsb load hbase-2 -p hbase.zookeeper.quorum=127.0.0.1 -p table=bobbytables -p columnfamily=col1 -p autoflush=true -P workloads/workloadb -threads 3 -s
 * ./bin/ycsb run hbase-2 -p hbase.zookeeper.quorum=127.0.0.1 -p table=bobbytables -p columnfamily=col1 -p autoflush=true -P workloads/workloadb -threads 3 -s
 */
public class HBaseClient2 extends com.yahoo.ycsb.DB{
	private Configuration config;
	private String zookeeperQuorum;
	private boolean autoFlush = false;
	private boolean debug = false;
	private byte[] columnfamily = Bytes.toBytes("family");
	private static Logger logger = Logger.getLogger(HBaseClient2.class);
	private Map<String,HTable> tables = new HashMap<String, HTable>();

	public void createTable(String tableName, String columnsFamilyName) throws IOException {
		HBaseAdmin hBaseAdmin = new HBaseAdmin(this.config);
		HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
		desc.addFamily( new HColumnDescriptor(Bytes.toBytes(columnsFamilyName)) );
		hBaseAdmin.createTable(desc);
	}

	private HTable getTable(String tableName) throws IOException {
		if(!this.tables.containsKey(tableName)){
			HTable t = new HTable(config, TableName.valueOf(tableName));
			t.setAutoFlushTo(this.autoFlush);
			logger.info("NEW TABLE OBJECT "+Thread.currentThread().toString()+" "+tableName);
			if(!t.getTableDescriptor().hasFamily(this.columnfamily)){
				logger.error("Table "+tableName+" does not have a columnfamily named "+
						Bytes.toString(columnfamily));
				throw new IllegalStateException("Table "+tableName+
						" does not have a columnfamily named "+
						Bytes.toString(columnfamily));
			}
			this.tables.put(tableName, t);
			return t;
		}
		return this.tables.get(tableName);
	}

	@Override
	public void cleanup() throws DBException {
		super.cleanup();
		IOException ex = null;
		for(Map.Entry<String,HTable> e: this.tables.entrySet()){
			logger.info("closing HTable "+e.getKey());
			try {
				long start = System.nanoTime();
				e.getValue().flushCommits();
				long end = System.nanoTime();
				if(this.autoFlush == false) {
					Measurements.getMeasurements().measure("UPDATE", (int) ((end - start) / 1000));
				}
				logger.info("Flushed HTable "+e.getKey()+" in "+((end - start) / (1000*1000))+"ms");
				e.getValue().close();
			} catch (IOException e1) {
				logger.error("Error while closing HTable for table "+e.getKey(), e1);
				ex = e1;
			}
		}
		if(ex != null){
			throw new DBException(ex);
		}
	}

	protected void readParameters(){
		Properties p = this.getProperties();
		Preconditions.checkNotNull(p.getProperty("hbase.zookeeper.quorum"));
		this.zookeeperQuorum = p.getProperty("hbase.zookeeper.quorum");

		if(p.containsKey("autoflush")){
			String aflush = p.getProperty("autoflush").trim().toLowerCase();
			Preconditions.checkArgument(aflush.equals("true") || aflush.equals("false"));
			if(aflush.equals("true")){
				this.autoFlush = true;
			}else{
				this.autoFlush = false;
			}
		}
		logger.info("AUTOFLUSH: "+this.autoFlush);

		if(p.containsKey("debug")){
			String debug = p.getProperty("debug").trim().toLowerCase();
			Preconditions.checkArgument(debug.equals("true") || debug.equals("false"));
			if(debug.equals("true")){
				this.debug = true;
			}else{
				this.debug = false;
			}
		}
		logger.info("DEBUG: "+this.debug);

		if(p.containsKey("columnfamily")){
			String colfam = p.getProperty("columnfamily").trim();
			Preconditions.checkArgument(colfam.length() > 0);
			this.columnfamily = Bytes.toBytes(colfam);
		}

		logger.info("COLUMN FAMILY: "+Bytes.toString(this.columnfamily));
	}

	@Override
	public void init() throws DBException {
		super.init();
		this.readParameters();
		this.config = HBaseConfiguration.create();
		this.config.set("hbase.zookeeper.quorum", this.zookeeperQuorum);
	}

	@Override
	public int read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
		try {
			HTable t = this.getTable(table);
			Get g = new Get(Bytes.toBytes(key));

			if(fields != null){
				for(String f: fields) {
					g.addColumn(this.columnfamily, Bytes.toBytes(f));
				}
			}else{
				g.addFamily(this.columnfamily);
			}

			Result result1 = t.get(g);
			if(result1.isEmpty()){
				return 1;
			}
			for(Map.Entry<byte[], byte[]> e : result1.getFamilyMap(this.columnfamily).entrySet()){
				result.put(Bytes.toString(e.getKey()), new ByteArrayByteIterator(e.getValue()));
			}
		} catch (IOException e) {
			logger.error("EXCEPTION WHILE READING"+key+" to "+table, e);
			throw new RuntimeException(e);
		}
		return 0;
	}

	@Override
	public int scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
		try {
			HTable t = this.getTable(table);

			Scan c = new Scan(Bytes.toBytes(startkey));
			c.setCaching(recordcount);
			if(fields != null){
				for(String f: fields) {
					c.addColumn(this.columnfamily, Bytes.toBytes(f));
				}
			}else{
				c.addFamily(this.columnfamily);
			}

			ResultScanner scanner = t.getScanner(c);

			for(int i = 0; i< recordcount; i++){
				Result r = scanner.next();
				if(r == null || r.isEmpty()){
					if(i==0){
						return 1;
					}
					break;
				}
				HashMap<String, ByteIterator> m = new HashMap<String,ByteIterator>();
				for(Map.Entry<byte[], byte[]> e : r.getFamilyMap(this.columnfamily).entrySet()){
					m.put(Bytes.toString(e.getKey()), new ByteArrayByteIterator(e.getValue()));
				}
				result.add(m);
			}

		} catch (IOException e) {
			logger.error("EXCEPTION WHILE SCANNING "+recordcount+"records from "+startkey+" in "+table, e);
			throw new RuntimeException(e);
		}


		return 0;
	}

	@Override
	public int update(String table, String key, HashMap<String, ByteIterator> values) {

		try {
			HTable t = this.getTable(table);
			Put p = new Put(Bytes.toBytes(key));
			for(Map.Entry<String,ByteIterator> e: values.entrySet()){
				p.add(this.columnfamily, Bytes.toBytes(e.getKey()), e.getValue().toArray());
			}
			t.put(p);
		} catch (IOException e) {
			logger.error("EXCEPTION WHILE WRITING "+key+" to "+table, e);
			throw new RuntimeException(e);
		}
		return 0;
	}

	@Override
	public int insert(String table, String key, HashMap<String, ByteIterator> values) {
		return this.update(table, key, values);
	}

	@Override
	public int delete(String table, String key) {
		try {
			HTable t = this.getTable(table);

			Delete d = new Delete(Bytes.toBytes(key));
			t.delete(d);
		} catch (IOException e) {
			logger.error("EXCEPTION WHILE DELETING "+key+" in "+table, e);
			throw new RuntimeException(e);
		}
		return 0;
	}
}
