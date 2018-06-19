package com.yahoo.ycsb.db.ignite;

import com.yahoo.ycsb.DB;
import org.apache.ignite.Ignite;
import org.junit.After;
import org.junit.AfterClass;

/**
 * Common test class.
 */
public class IgniteClientCommonTest {
  /** */
  protected static Ignite cluster;

  /** */
  protected DB client;

  /**
   *
   */
  @After
  public void tearDown() throws Exception {
    client.cleanup();
  }

  /**
   *
   */
  @AfterClass
  public static void afterClass() {
    cluster.close();

    try {
      Thread.sleep(1000);
    }
    catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}
