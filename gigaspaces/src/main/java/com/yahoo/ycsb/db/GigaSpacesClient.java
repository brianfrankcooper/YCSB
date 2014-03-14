package com.yahoo.ycsb.db;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.openspaces.core.GigaSpace;
import org.openspaces.core.GigaSpaceConfigurer;
import org.openspaces.core.space.UrlSpaceConfigurer;
import org.springframework.dao.DataAccessException;

import com.j_spaces.core.client.UpdateModifiers;
import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.Client;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.StringByteIterator;
import com.yahoo.ycsb.Workload;
import com.yahoo.ycsb.db.Data;
import com.yahoo.ycsb.db.gigaspaces.model.Class0Index;

/**
 * GigaSpaces XAP client for the YCSB benchmark.<br />
 * <p>
 * Acts as a GigaSpaces client. Support any Data Grid topology and client local
 * cache.
 * </p>
 * <p>
 * GigaSpace space URL can be provided using <code>gigspaces.url=URL</code>
 * property on the YCSB command line or via YCSB config file. Examples for Space
 * URL can be found at: <a
 * href="http://www.gigaspaces.com/wiki/display/XAP8/Space+URL"> Space URL wiki
 * page</a>
 * </p>
 * <p>
 * The <code>gigspaces.warmup=true</code> property can be used to warm up the
 * Space.
 * </p>
 * <p>
 * Benchmark results can be found at: <a
 * href="http://www.gigaspaces.com/benchmarks"> Benchmarks Page</a>
 * </p>
 * 
 */
public class GigaSpacesClient extends CacheClient {

	public static void reset(){
		counter.set(0);
		gigaSpace=null;
		onInit.set(false);
		doneInit.set(false);
	}
	private static final String SPACE_URL_PROP = "gigspaces.url";
	private static final String WARMUP_PROP = "gigspaces.warmup";

	private static String spaceURL = "/./mySpace";

	private static GigaSpace gigaSpace;
	static AtomicBoolean onInit = new AtomicBoolean(false);
	static AtomicBoolean doneInit = new AtomicBoolean(false);

	Data dataArray[] = null;
	Class0Index dataArrayBatch[] = null;
	Class0Index template;
	@Override
	public void init() throws DBException {

		synchronized (onInit) {
			if (gigaSpace == null) {
				if (!onInit.getAndSet(true)) {
					Properties props = getProperties();
					boolean warmpup = false;

					if (props != null && !props.isEmpty()) {
						if (props.containsKey(SPACE_URL_PROP))
							spaceURL = props.getProperty(SPACE_URL_PROP);

						if (props.getProperty(WARMUP_PROP, "false")
								.equalsIgnoreCase("true"))
							warmpup = true;
					}
					System.out.println("Initialing space proxy");
					gigaSpace = new GigaSpaceConfigurer(new UrlSpaceConfigurer(
							spaceURL)).gigaSpace();
					gigaSpace.getSpace().setUpdateModifiers(
							UpdateModifiers.NO_RETURN_VALUE
									| UpdateModifiers.UPDATE_OR_WRITE);

					regularPayloadMode = props.getProperty(PAYLOAD_TYPE_PROP,
							"regular").equalsIgnoreCase("regular");

					batchMode = Boolean.valueOf(
							props.getProperty(Client.BATCH_MODE_PROPERTY,
									"false")).booleanValue();
					batchSize = Integer
							.valueOf(
									props.getProperty(
											Client.BATCH_SIZE_PROPERTY, "1000"))
							.intValue();

					
					if (!regularPayloadMode) {
						try {
							dataClass = Class.forName(props
									.getProperty(PAYLOAD_CLASS_TYPE_PROP));
						} catch (ClassNotFoundException e) {
							e.printStackTrace();
							throw new DBException(e);
						}
					}

					if (warmpup)
						warmup();

					if (props.getProperty(CLEAR_BEFORE_PROP, "false")
							.equalsIgnoreCase("true")) {
						try {
							gigaSpace.clear(dataClass.newInstance());
						} catch (DataAccessException e) {
							e.printStackTrace();
						} catch (InstantiationException e) {
							e.printStackTrace();
						} catch (IllegalAccessException e) {
							e.printStackTrace();
						}
					}
					printScenario();
					System.out.println("----------- GigaSpaces has " + gigaSpace.count(null) + " objects ----------");

					doneInit.set(true);
				} else {
					while (!doneInit.get()) {
						try {
							Thread.sleep(100);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
				}
			}
		}
		if (batchMode)
		{
			if (regularPayloadMode)
			{
				dataArray = new Data[batchSize];
			
				for (int i=0;i<batchSize;i++)
				{
					dataArray[i] = new Data();
				}
			}
			else
			{

				dataArrayBatch = new Class0Index[batchSize];
				for (int i=0;i<batchSize;i++)
				{
					try {
						dataArrayBatch[i] = (Class0Index) dataClass.newInstance();
					} catch (InstantiationException e) {
						e.printStackTrace();
					} catch (IllegalAccessException e) {
						e.printStackTrace();
					}
				}
			}
		}
		else
		{
			if (!regularPayloadMode)
			{
				try {
					template = (Class0Index) dataClass.newInstance();
				} catch (InstantiationException e) {
					e.printStackTrace();
				} catch (IllegalAccessException e) {
					e.printStackTrace();
				}
			}			
		}
		
	}

	private void warmup() {

		System.out.println("Start warmup");
		for (int i = 0; i < 10000; i++) {
			Data d = new Data();
			d.setId(i + "");
			gigaSpace.write(d);
		}
		// gigaSpace.clear(new Data());
		System.out.println("Done warmup");
	}

	@Override
	public int read(String table, String key, Set<String> fields,
			HashMap<String, ByteIterator> result) {
		
		if (batchMode)
		{
			if (regularPayloadMode)
			{
				try {
					Object ret[] = gigaSpace.readMultiple(dataClass.newInstance(),batchSize);
					if (ret.length >= batchSize) return SUCCESS; 
				} catch (DataAccessException e) {
					e.printStackTrace();
				} catch (InstantiationException e) {
					e.printStackTrace();
				} catch (IllegalAccessException e) {
					e.printStackTrace();
				}
			}
			else
			{
					template.setAttrib2(counter.incrementAndGet() % 100);
					Object ret[] = gigaSpace.readMultiple(template,batchSize);
					if (ret.length == batchSize) 
						return SUCCESS; 
					else
					{
						System.out.println("Batch Read - Can't find matching objects");
						return ERROR; 
					}
			}
		}
		else
		{
			Object o = null;
			if (regularPayloadMode)
			{
				o = gigaSpace.readById(dataClass, key);
			}
			else
			{
				// indexed payload
				template.setAttrib2(counter.incrementAndGet() % 100);
				o = gigaSpace.read(template);
			}
			if (o != null)
				return SUCCESS;
			}
		return ERROR;
	}

	@Override
	public int scan(String table, String startkey, int recordcount,
			Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {

		return ERROR;
	}

	@Override
	public int update(String table, String key,
			HashMap<String, ByteIterator> values) {

		if (regularPayloadMode) {
			if (batchMode) {
				
				Map<String, byte[]> val = convertToBytearrayMap(values);

				for (int i = 0; i < batchSize; i++) {
					dataArray[i].setId(key + "_" + i);
					dataArray[i].setData(val);
				}
				gigaSpace.writeMultiple(dataArray);
				return SUCCESS;
			}
			// Single mode
			else {
				Data data = new Data();
				data.setId(key);
				data.setData(convertToBytearrayMap(values));
				gigaSpace.write(data);
			}
		}
		// Indexed mode
		else {
			if (batchMode) {
				int counter_ = counter.incrementAndGet();
				for (int i = 0; i < batchSize; i++) {
					dataArrayBatch[i].setId(key + "_" + i);
					dataArrayBatch[i].setAttrib1(key);
					dataArrayBatch[i].setAttrib2(counter_ % 100);
					dataArrayBatch[i].setAttrib3((long) counter_ % 50);
					dataArrayBatch[i].setAttrib4((double) counter_ % 20);
				}
				gigaSpace.writeMultiple(dataArrayBatch);
				return SUCCESS;

			} else {
				try {
					Class0Index data;
					int counter_ = counter.incrementAndGet();
					data = (Class0Index) dataClass.newInstance();
					data.setId(key);
					data.setAttrib1(key);
					data.setAttrib2(counter_ % 100);
					data.setAttrib3((long) counter_ % 50);
					data.setAttrib4((double) counter_ % 20);

					gigaSpace.write(data);
				} catch (InstantiationException e) {
					e.printStackTrace();
				} catch (IllegalAccessException e) {
					e.printStackTrace();
				}
			}
		}
		return SUCCESS;
	}

	@Override
	public int insert(String table, String key,
			HashMap<String, ByteIterator> values) {
		return update(table, key, values);
	}

	@Override
	public int delete(String table, String key) {
		Data template = new Data();
		template.setId(key);
		gigaSpace.clear(template);
		return SUCCESS;
	}
}