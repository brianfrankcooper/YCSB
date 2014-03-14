package com.yahoo.ycsb.db;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicBoolean;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionExistsException;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.client.ClientRegionFactory;
import com.gemstone.gemfire.cache.client.ClientRegionShortcut;
import com.gemstone.gemfire.cache.query.FunctionDomainException;
import com.gemstone.gemfire.cache.query.NameResolutionException;
import com.gemstone.gemfire.cache.query.QueryInvocationTargetException;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.TypeMismatchException;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.Client;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.db.gigaspaces.model.Class0Index;

/**
 * VMware vFabric GemFire client for the YCSB benchmark.<br />
 * <p>
 * By default acts as a GemFire client and tries to connect to GemFire cache
 * server running on localhost with default cache server port. The
 * <code>gemfireclientconfig<code> property sets the client <code>cache-xml-file<code> location.
 * The <code>gemfireP2Pclient<code> property set to <code>true<code> will run the benchmark in P2P mode.
 * </p>
 * <p>
 * YCSB by default does its operations against the "usertable" region. When
 * running as a client this is a <code>ClientRegionShortcut.PROXY</code> region,
 * when running in p2p mode it is a <code>RegionShortcut.PARTITION</code>
 * region.
 * </p>
 * 
 */
public class GemFireClient extends CacheClient {
	public static void reset() {
		counter.set(0);
		onInit.set(false);
		doneInit.set(false);
	}

	// private static GemFireCache cache;

	/**
	 * true if ycsb client runs as a client to a GemFire cache server
	 */
	private boolean isClient = true;

	private static final String GEMFIRE_CLIENT_CONFIG_PROP = "gemfireclientconfig";
	private static final String CLIENT_P2P_PROP = "gemfireP2Pclient";

	static AtomicBoolean onInit = new AtomicBoolean(false);
	static AtomicBoolean doneInit = new AtomicBoolean(false);

	private static Region region;

	@Override
	public void init() throws DBException {
		synchronized (onInit) {
			if (region == null) {
				if (!onInit.getAndSet(true)) {
					Properties props = getProperties();

					batchMode = Boolean.valueOf(
							props.getProperty(Client.BATCH_MODE_PROPERTY,
									"false")).booleanValue();

					isClient = !(Boolean.valueOf(props.getProperty(
							CLIENT_P2P_PROP, "false")).booleanValue());

					batchSize = Integer.valueOf(
							props.getProperty(Client.BATCH_SIZE_PROPERTY,
									"1000")).intValue();

					if (!props.containsKey(GEMFIRE_CLIENT_CONFIG_PROP)) {
						System.out.println("No Gemfire client config - Exit");
						System.exit(1);
					}
					// /////////////////////////////
					if (isClient) {
						System.out
								.println("--------- Running in a remote Client mode --------- ");
						ClientCache cache = new ClientCacheFactory()
								.set("name", "BenchmarkClient")
								.set("cache-xml-file",
										props.getProperty(GEMFIRE_CLIENT_CONFIG_PROP))
								.create();

						region = cache.getRegion("usertable");
					} else {
						System.out
								.println("--------- Running in a P2P mode --------- ");
						Cache cache = new CacheFactory()
								.set("name", "BenchmarkClientP2P")
								.set("cache-xml-file",
										props.getProperty(GEMFIRE_CLIENT_CONFIG_PROP))
								.create();
						region = cache.getRegion("usertable");
					}
					if (props.getProperty(CLEAR_BEFORE_PROP, "false")
							.equalsIgnoreCase("true")) {
						region.clear();
					}

					regularPayloadMode = props.getProperty(PAYLOAD_TYPE_PROP,
							"regular").equalsIgnoreCase("regular");
					if (!regularPayloadMode) {
						System.out.println("Running in indexed mode");

						try {
							dataClass = Class.forName(props
									.getProperty(PAYLOAD_CLASS_TYPE_PROP));
						} catch (ClassNotFoundException e) {
							e.printStackTrace();
							throw new DBException(e);
						}
					} else {
						System.out.println("Running in regular mode");
					}
					printScenario();
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

		System.out.println("----------- " + System.currentTimeMillis()
				+ " GemFire has " + region.size() + " keys ----------");

	}

	int query(String table, int limit) {
		SelectResults queryResult = null;
		String queryStr = null;
		int counter_ = counter.incrementAndGet();
		if (regularPayloadMode) {
			queryStr = "select * from /" + table + " limit " + limit;
		} else {
			queryStr = "select * from /" + table + " where attrib2="
					+ (counter_ % 100) + " limit " + limit;
		}

		try {
			queryResult = region.query(queryStr);
		} catch (FunctionDomainException e) {
			e.printStackTrace();
		} catch (TypeMismatchException e) {
			e.printStackTrace();
		} catch (NameResolutionException e) {
			e.printStackTrace();
		} catch (QueryInvocationTargetException e) {
			e.printStackTrace();
		}
		return queryResult.size();
	}

	@Override
	public int read(String table, String key, Set<String> fields,
			HashMap<String, ByteIterator> result) {
		if (batchMode) {
			int res = query(table, batchSize);
			if (res >= batchSize) {
				return SUCCESS;
			} else {
				System.out.println("Batch Read - Can't find " + batchSize
						+ " matching objects. Query found " + res
						+ " matching objects");
				return ERROR;
			}
		}
		// single mode
		if (regularPayloadMode) {
			Object val = region.get(key);

			if (val != null) {
				return SUCCESS;
			}
		} else
		// indexed mode
		{
			int res = query(table, 1);
			if (res == 1) {
				return SUCCESS;
			} else {
				System.out
						.println("Single Read - Can't find one matching objects. Query found "
								+ res + " matching objects");
				return ERROR;
			}
		}
		return ERROR;
	}

	@Override
	public int scan(String table, String startkey, int recordcount,
			Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
		// GemFire does not support scan
		return ERROR;
	}

	@Override
	public int update(String table, String key,
			HashMap<String, ByteIterator> values) {
		insert(table, key, values);
		return SUCCESS;
	}

	@Override
	public int insert(String table, String key,
			HashMap<String, ByteIterator> values) {

		if (batchMode) {
			if (regularPayloadMode) {
				Map<String, byte[]> val = convertToBytearrayMap(values);
				HashMap<String, Object> map = new HashMap<String, Object>();
				for (int i = 0; i < batchSize; i++) {
					map.put(key + "_" + i, val);
				}
				region.putAll(map);
				return SUCCESS;
			} else {
				// Batch indexed mode
				HashMap<String, Object> map = new HashMap<String, Object>();
				Class0Index data;
				int counter_ = counter.incrementAndGet();
				for (int i = 0; i < batchSize; i++) {
					try {
						data = (Class0Index) dataClass.newInstance();
						data.setId(key + "_" + i);
						data.setAttrib1(key);
						data.setAttrib2(counter_ % 100);
						data.setAttrib3((long) counter_ % 50);
						data.setAttrib4((double) counter_ % 20);
						map.put(key + "_" + i, data);
					} catch (InstantiationException e) {
						e.printStackTrace();
					} catch (IllegalAccessException e) {
						e.printStackTrace();
					}
				}
				region.putAll(map);
				return SUCCESS;
			}
		}

		// single mode
		if (regularPayloadMode) {
			region.put(key, convertToBytearrayMap(values));
			return SUCCESS;
		} else {
			Class0Index data;
			try {
				int counter_ = counter.incrementAndGet();
				data = (Class0Index) dataClass.newInstance();
				data.setId(key);
				data.setAttrib1(key);
				data.setAttrib2(counter_ % 100);
				data.setAttrib3((long) counter_ % 50);
				data.setAttrib4((double) counter_ % 20);
				region.put(key, data);
			} catch (InstantiationException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			}
		}
		// System.out.println("insert Key="+ key + " ret ="+ ret);
		return SUCCESS;
	}

	@Override
	public int delete(String table, String key) {
		region.destroy(key);
		return 0;
	}

}
