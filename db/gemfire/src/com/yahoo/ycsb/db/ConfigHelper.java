package com.yahoo.ycsb.db;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import com.gemstone.gemfire.cache.util.ObjectSizer;
import com.yahoo.ycsb.workloads.CoreWorkload;

public class ConfigHelper {

  public static void main(String[] args) throws IOException {
    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
    System.out.print("Enter the heap size for GemFire in GB:");
    String input = null;
    input = br.readLine();
    long bytes = convertToBytes(input);
    
    System.out.print("Enter % of heap to be filled: (default:90):");
    input = br.readLine();
    int percent = 90;
    if (!input.equals("")) {
      percent = Integer.parseInt(input.trim());
    }
    
    long bytesToFill = (bytes*percent)/100;
    
    System.out.println("Bytes available:"+bytes);
    System.out.println("Bytes to fill  :"+bytesToFill);
    
    System.out.print("Enter field count for value: (default:"+CoreWorkload.FIELD_COUNT_PROPERTY_DEFAULT+"):");
    input = br.readLine();
    int fieldCount = 0;
    if (input.equals("")) {
      fieldCount = Integer.parseInt(CoreWorkload.FIELD_COUNT_PROPERTY_DEFAULT);
    } else {
      fieldCount = Integer.parseInt(input.trim());
    }
    
    System.out.print("Enter field length in bytes: (default:"+CoreWorkload.FIELD_LENGTH_PROPERTY_DEFAULT+"):");
    input = br.readLine();
    int fieldLength = 0;
    if (input.equals("")) {
      fieldLength = Integer.parseInt(CoreWorkload.FIELD_LENGTH_PROPERTY_DEFAULT);
    } else {
      fieldLength = Integer.parseInt(input.trim());
    }
    
    ObjectSizer s = ObjectSizer.DEFAULT;
    String keyPrefix = "user";
    String valPrefix = "field";
    int sizeOfInt = 4;
    int keySize = s.sizeof(keyPrefix) + sizeOfInt;
    int valSize = (s.sizeof(valPrefix) + sizeOfInt + fieldLength) * fieldCount;
    long entrySize = keySize + valSize;
    System.out.println("Entry size in bytes:"+entrySize+"\n");
    
    System.out.println("recordcount="+(bytesToFill/entrySize));
  }

  private static long convertToBytes(String input) {
    input = input.trim();
    String b = null;
    long bytes = 0;
    if (input.contains("g") || input.contains("G")) {
      b = input.substring(0, input.length()-1);
    } else {
      b = input;
    }
    long g = Long.parseLong(b);
    bytes = g * 1024 * 1024 * 1024;
    return bytes;
  }
}
