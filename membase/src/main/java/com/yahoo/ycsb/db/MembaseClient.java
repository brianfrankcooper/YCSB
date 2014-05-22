package com.yahoo.ycsb.db;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.StringByteIterator;
import net.spy.memcached.CASResponse;
import net.spy.memcached.MemcachedClient;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

public class MembaseClient extends DB {
	MemcachedClient client;

    protected static final ObjectMapper MAPPER = new ObjectMapper();

	public static final String VERBOSE = "memcached.verbose";
	public static final String VERBOSE_DEFAULT = "true";

	public static final String SIMULATE_DELAY = "memcached.simulatedelay";
	public static final String SIMULATE_DELAY_DEFAULT = "0";

	public static final String MEMCACHED_ADDRESS = "memcached.address";
	public static final String MEMCACHED_ADDRESS_DEFAULT = "127.0.0.1";

	public static final String MEMBASE_BUCKET = "membase.bucket";
	public static final String MEMBASE_BUCKET_DEFAULT = "test_bucket1";

	public static final String MEMBASE_PASSWORD = "membase.password";
	public static final String MEMBASE_PASSWORD_DEFAULT = "";

	public static long endtime;
	
	Random random;
	boolean verbose;
	int todelay;

	public MembaseClient() {
		random = new Random();
		todelay = 0;
	}
	
	/**
	 * Initialize any state for this DB. Called once per DB instance; there is
	 * one DB instance per client thread.
	 */
	public void init() {
		String address = getProperties().getProperty(MEMCACHED_ADDRESS, MEMCACHED_ADDRESS_DEFAULT);
		String bucketName = getProperties().getProperty(MEMBASE_BUCKET, MEMBASE_BUCKET_DEFAULT);
		String password = getProperties().getProperty(MEMBASE_PASSWORD, MEMBASE_PASSWORD_DEFAULT);
		verbose = Boolean.parseBoolean(getProperties().getProperty(VERBOSE, VERBOSE_DEFAULT));
		todelay = Integer.parseInt(getProperties().getProperty(SIMULATE_DELAY, SIMULATE_DELAY_DEFAULT));

		try {
			List<URI> uris = new LinkedList<URI>();
			uris.add(new URI("http://"+ address +"/pools"));
			client = new MemcachedClient(uris, bucketName, password);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e1) {
			e1.printStackTrace();
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
	}
	
	public void cleanup() {
		if (client.isAlive())
			client.shutdown();
	}

    public int add(String key, HashMap<String, ByteIterator> values) {
		try {
			if (!client.add(key, 0, toJson(values)).get().booleanValue())
				return -1;
		} catch (InterruptedException e) {
			System.out.println("ADD Interrupted");
            e.printStackTrace();
		} catch (ExecutionException e) {
			System.out.println("ADD Execution");
            e.printStackTrace();
		} catch (RuntimeException e) {
			System.out.println("ADD Runtime");
            e.printStackTrace();
		} catch (IOException e) {
            System.out.println("ADD IO");
            e.printStackTrace();
        }
        return 0;
	}
	
	public int get(String key, Object value) {
		Future<Object> f = client.asyncGet(key);
		//long time = System.nanoTime();
		try {
			if (f.get() == null) {
				System.out.println("Error");
				return -1;
			}
		} catch (InterruptedException e) {
			System.out.println("GET Interrupted");
		} catch (ExecutionException e) {
			System.out.println("GET Execution");
			e.printStackTrace();
			return -2;
		} catch (RuntimeException e) {
			System.out.println("GET Runtime");
			return -3;
		}
		//System.out.println("Start: " + time);
		//System.out.println("Start: " + endtime);
		//System.out.println("Spy latency: " + ((endtime - time)/1000));
		return 0;
	}
	/*
	public Future<Object> asyncGet(final String key) {
		return asyncGet(key, client.getTranscoder());
	}
	
	public <T> Future<T> asyncGet(final String key, final Transcoder<T> tc) {
		final CountDownLatch latch=new CountDownLatch(1);
		final GetFuture<T> rv=new GetFuture<T>(latch, 1000);
		
		Operation op=client.opFact.get(key,
				new GetOperation.Callback() {
			private Future<T> val=null;
			public void receivedStatus(OperationStatus status) {
				rv.set(val);
				
			}
			public void gotData(String k, int flags, byte[] data) {
				assert key.equals(k) : "Wrong key returned";
				val=client.tcService.decode(tc,
					new CachedData(flags, data, tc.getMaxSize()));
			}
			
			public void complete() {
				SpymemcachedClient.endtime = System.nanoTime();
				System.out.println("Complete");
				latch.countDown();
			}});
		rv.setOperation(op);
		client.addOp(key, op);
		return rv;
	}*/
	
	
	

	public int set(String key, Object value) {
		try {
			if (!client.set(key, 0, value).get().booleanValue())
				return -1;
		} catch (InterruptedException e) {
			System.out.println("SET Interrupted");
		} catch (ExecutionException e) {
			System.out.println("SET Execution");
		} catch (RuntimeException e) {
			System.out.println("SET Runtime");
		}
		return 0;
	}
	
	private byte[] ipv4AddressToByte(String address) {
		byte[] b = new byte[4];
		String[] str = address.split("\\.");
		b[0] = Integer.valueOf(str[0]).byteValue();
		b[1] = Integer.valueOf(str[1]).byteValue();
		b[2] = Integer.valueOf(str[2]).byteValue();
		b[3] = Integer.valueOf(str[3]).byteValue();
		return b;
	}

	public int append(String key, long cas, Object value) {
		try {
			if (!client.append(cas, key, value).get().booleanValue())
				return -1;
		} catch (InterruptedException e) {
			System.out.println("APPEND Interrupted");
		} catch (ExecutionException e) {
			System.out.println("APPEND Execution");
		} catch (RuntimeException e) {
			System.out.println("APPEND Runtime");
		}
		return 0;
	}

	public int cas(String key, long cas, Object value) {
		if (!client.cas(key, cas, value).equals(CASResponse.OK))
			return -1;
		return 0;
	}

	public int decr(String key, Object value) {
		return 0;
	}

	public int delete(String key) {
		return 0;
	}

	public int incr(String key, Object value) {
		return 0;
	}

	public long gets(String key) {
		long cas = client.gets(key).getCas();
		if (cas < 0)
			return -1;
		return cas;
	}

	public int prepend(String key, long cas, Object value) {
		try {
			if (!client.prepend(cas, key, value).get().booleanValue())
				return -1;
		} catch (InterruptedException e) {
			System.out.println("PREPEND Interrupted");
		} catch (ExecutionException e) {
			System.out.println("PREPEND Execution");
		} catch (RuntimeException e) {
			System.out.println("PREPEND Runtime");
		}
		return 0;
	}

	public int replace(String key, Object value) {
		try {
			if (!client.replace(key, 0, value).get().booleanValue())
				return -1;
		} catch (InterruptedException e) {
			System.out.println("REPLACE Interrupted");
		} catch (ExecutionException e) {
			System.out.println("REPLACE Execution");
		} catch (RuntimeException e) {
			System.out.println("REPLACE Runtime");
		}
		return 0;
	}
	
	public static void main(String args[]) {
		MembaseClient client = new MembaseClient();
		client.init();
	}

    @Override
    public int read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
        return get(key, null);
    }

    @Override
    public int scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        return 0;
    }

    @Override
    public int update(String table, String key, HashMap<String, ByteIterator> values) {
        return set(key, values);
    }

    @Override
    public int insert(String table, String key, HashMap<String, ByteIterator> values) {
        return add(key, values);
    }

    @Override
    public int delete(String table, String key) {
        return delete(key);
    }

    protected static String toJson(Map<String, ByteIterator> values) throws IOException {
        ObjectNode node = MAPPER.createObjectNode();
        HashMap<String, String> stringMap = StringByteIterator.getStringMap(values);
        for (Map.Entry<String, String> pair : stringMap.entrySet()) {
            node.put(pair.getKey(), pair.getValue());
        }
        JsonFactory jsonFactory = new JsonFactory();
        Writer writer = new StringWriter();
        JsonGenerator jsonGenerator = jsonFactory.createJsonGenerator(writer);
        MAPPER.writeTree(jsonGenerator, node);
        return writer.toString();
    }

}
