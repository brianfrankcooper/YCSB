package com.yahoo.ycsb.db;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.regionserver.StoreFile.BloomType;

public class HbaseAdminClient
{
  public enum Operation
  {
    CREATE, DROP, ENABLE, DISABLE
  }

  private static final String PARAM_NAME_TABLE = "-table=";
  private static final String PARAM_NAME_REGIONS = "-regions=";
  private static final String PARAM_NAME_FAMILY = "-family";

  Configuration _configuration;
  HBaseAdmin _admin;
  String _compression = null;

  private int _regions = 200;

  private String _tablename = "usertable";
  private String _family = "family";

  private Operation _operation;

  public static void main(String[] args) throws IOException, MasterNotRunningException
  {
    if (args.length == 0) {
      printUsageAndExit(1);
    }
    HbaseAdminClient.parseArgs(args).run();
  }

  private void run() throws IOException
  {
    _configuration = HBaseConfiguration.create();
    System.out.printf("%s\n", new Object[] { _configuration.toString() });
    _admin = new HBaseAdmin(_configuration);

    switch (_operation)
    {
    case CREATE:
      createTable();
      break;
    case DROP:
      dropTable();
      break;
    case ENABLE:
      enableTable();
      break;
    case DISABLE:
      disableTable();
      break;
    }

    _admin.close();
  }

  private void disableTable() throws IOException
  {
    if (_admin.isTableEnabled(_tablename)) {
      System.out.println("Disabling table: " + _tablename);
      try {
        _admin.disableTable(_tablename);
        System.out.println("Table: " + _tablename + " disabled");
      }
      catch (IOException e) {
        System.err.println("Error disabling table: " + _tablename + ".\n" + e.getMessage());
        throw e;
      }
    }
    else {
      System.out.println("Table: " + _tablename + " is already disabled");
    }
  }

  private void enableTable() throws IOException
  {
    if (_admin.isTableDisabled(_tablename)) {
      System.out.println("Enabling table: " + _tablename);
      try {
        _admin.enableTable(_tablename);
        System.out.println("Table: " + _tablename + " enabled.");
      }
      catch (IOException e) {
        System.err.println("Error enabling table: " + _tablename + ".\n" + e.getMessage());
        throw e;
      }
    }
    else {
      System.out.println("Table: " + _tablename + " is already enabled.");
    }
  }

  private void dropTable() throws IOException
  {
    disableTable();
    System.out.println("Deleting table: " + _tablename);
    try {
      _admin.deleteTable(_tablename);
      System.out.println("Table: " + _tablename + " deleted.");
    }
    catch (IOException e) {
      System.err.println("Error deleting table: " + _tablename + ".\n" + e.getMessage());
      throw e;
    }
  }

  private void createTable() throws IOException
  {
    if (_admin.tableExists(_tablename)) {
      System.out.println("Table: " + _tablename + " already exists.");
      return;
    }
    System.out.println("Creating table: " + _tablename + " with column family: " +
        _family + " and total " + _regions + " regions.");
    HTableDescriptor descriptor = new HTableDescriptor(_tablename);
    HColumnDescriptor family = new HColumnDescriptor(_family);
    family.setBloomFilterType(BloomType.ROW);
    descriptor.addFamily(family);

    long start = 0;
    long end = (1L << 31);
    long delta = (end - start) / (_regions + 1);
    List<String> splitKeys = new ArrayList<String>();
    long split = start;
    for (int i = 0; i < _regions; i++) {
      splitKeys.add("user" + split);
      split += delta;
    }
    Collections.sort(splitKeys);
    System.out.printf("%s\n", splitKeys);

    byte[][] splits = new byte[_regions][];
    int i = 0;
    for (String key : splitKeys) {
      splits[(i++)] = key.getBytes();
    }
    try {
      _admin.createTable(descriptor, splits);
      System.out.println("Table: " + _tablename + " created.");
    }
    catch (IOException e) {
      System.err.println("Error creating table: " + _tablename + ".\n" + e.getMessage());
      throw e;
    }
  }

  private static HbaseAdminClient parseArgs(String[] args)
  {
    HbaseAdminClient tool = new HbaseAdminClient();
    try {
      tool._operation = Operation.valueOf(args[0].toUpperCase());
    }
    catch (Exception e) {
      System.out.println("Unknown operation: " + args[0]);
      printUsageAndExit(1);
    }
    for (int i = 1; i < args.length; i++) {
      if (args[i].startsWith(PARAM_NAME_TABLE)) {
        tool._tablename = args[i].substring(PARAM_NAME_TABLE.length());
      }
      else if (args[i].startsWith(PARAM_NAME_REGIONS)) {
        tool._regions = Integer.parseInt(args[i].substring(PARAM_NAME_REGIONS.length()));
      }
      else if (args[i].startsWith(PARAM_NAME_FAMILY)) {
        tool._family = args[i].substring(PARAM_NAME_FAMILY.length());
      }
      else {
        System.out.println("Unknown parameter: " + args[i]);
        printUsageAndExit(1);
      }
    }
    return tool;
  }

  private static void printUsageAndExit(int code)
  {
    StringBuilder sb = new StringBuilder();
    sb.append("HbaseAdminClient\n").append("Usage: HbaseAdminClient <Operation> [-param=value...]\n")
    .append("\tAvailable operations: ").append(Arrays.toString(Operation.values())).append("\n")
    .append("\tPossible params: -table, -regions, -family");
    System.out.println(sb.toString());

    System.exit(code);
  }
}
