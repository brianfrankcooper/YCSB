package site.ycsb.db.oxia;

import static io.streamnative.oxia.testcontainers.OxiaContainer.DEFAULT_IMAGE_NAME;
import static junit.framework.TestCase.assertEquals;
import static site.ycsb.db.oxia.OxiaClient.CONF_KEY_NAMESPACE;
import static site.ycsb.db.oxia.OxiaClient.CONF_KEY_SERVER_URL;
import static site.ycsb.workloads.CoreWorkload.TABLENAME_PROPERTY;
import static site.ycsb.workloads.CoreWorkload.TABLENAME_PROPERTY_DEFAULT;
import io.streamnative.oxia.testcontainers.OxiaContainer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.testcontainers.containers.Network;
import site.ycsb.ByteIterator;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;
import site.ycsb.measurements.Measurements;
import site.ycsb.workloads.CoreWorkload;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public final class OxiaStandaloneTest {
  private static final String NETWORK_ALIAS = "oxia";
  private static final Network network = Network.newNetwork();
  private static final String path = "benchmark";
  private String tableName;


  private OxiaClient client;

  private static OxiaContainer standalone =
      new OxiaContainer(DEFAULT_IMAGE_NAME).withNetworkAliases(NETWORK_ALIAS)
          .withNetwork(network);

  @Before
  public void setUp() throws Exception {
    standalone.start();

    client = new OxiaClient();

    Properties p = new Properties();
    p.setProperty(CONF_KEY_SERVER_URL, standalone.getServiceAddress());
    p.setProperty(CONF_KEY_NAMESPACE, "default");

    Measurements.setProperties(p);
    final CoreWorkload workload = new CoreWorkload();
    workload.init(p);

    tableName = p.getProperty(TABLENAME_PROPERTY, TABLENAME_PROPERTY_DEFAULT);

    client.setProperties(p);
    client.init();
  }

  @After
  public void tearDown() throws Exception {
    client.cleanup();
    standalone.close();
  }


  @Test
  public void testOxiaClient() {
    // insert
    Map<String, String> m = new HashMap<>();
    String field1 = "field_1";
    String value1 = "value_1";
    m.put(field1, value1);
    Map<String, ByteIterator> result = StringByteIterator.getByteIteratorMap(m);
    client.insert(tableName, path, result);

    // read
    result.clear();
    Status status = client.read(tableName, path, null, result);
    assertEquals(Status.OK, status);
    assertEquals(1, result.size());
    assertEquals(value1, result.get(field1).toString());

    // update(the same field)
    m.clear();
    result.clear();
    String newVal = "value_new";
    m.put(field1, newVal);
    result = StringByteIterator.getByteIteratorMap(m);
    client.update(tableName, path, result);
    assertEquals(1, result.size());

    // Verify result
    result.clear();
    status = client.read(tableName, path, null, result);
    assertEquals(Status.OK, status);
    // here we only have one field: field_1
    assertEquals(1, result.size());
    assertEquals(newVal, result.get(field1).toString());

    // update(two different field)
    m.clear();
    result.clear();
    String field2 = "field_2";
    String value2 = "value_2";
    m.put(field2, value2);
    result = StringByteIterator.getByteIteratorMap(m);
    client.update(tableName, path, result);
    assertEquals(1, result.size());

    // Verify result
    result.clear();
    status = client.read(tableName, path, null, result);
    assertEquals(Status.OK, status);
    // here we have two field: field_1 and field_2
    assertEquals(2, result.size());
    assertEquals(value2, result.get(field2).toString());
    assertEquals(newVal, result.get(field1).toString());

    // delete
    status = client.delete(tableName, path);
    assertEquals(Status.OK, status);

    // Verify result
    result.clear();
    status = client.read(tableName, path, null, result);
    // No value return not found
    assertEquals(Status.NOT_FOUND, status);
    assertEquals(0, result.size());
  }
}
