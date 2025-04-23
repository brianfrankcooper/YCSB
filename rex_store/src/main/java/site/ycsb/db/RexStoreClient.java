// package com.yahoo.ycsb.db;

package site.ycsb.db;

import java.io.*;
import java.net.Socket;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

/** 
 * YCSB binding for rex_store.
 */
public class RexStoreClient extends DB {
  private static final String DEFAULT_HOST = "127.0.0.1";
  private static final int DEFAULT_PORT = 8000;
  private Socket socket;
  private DataInputStream in;
  private DataOutputStream out;
  private final ObjectMapper mapper = new ObjectMapper();

  @Override
  public void init() throws DBException {
    String host = getProperties().getProperty("rexstore.host", DEFAULT_HOST);
    int port = Integer.parseInt(getProperties().getProperty("rexstore.port", String.valueOf(DEFAULT_PORT)));

    try {
      socket = new Socket(host, port);
      in = new DataInputStream(socket.getInputStream());
      out = new DataOutputStream(socket.getOutputStream());
    } catch (IOException e) {
      throw new DBException("Unable to connect to RexStore", e);
    }
  }

  private void sendCommand(ObjectNode command) throws IOException {
    byte[] payload = mapper.writeValueAsBytes(command);
    out.writeInt(payload.length); // 4-byte big-endian
    out.write(payload);
    out.flush();
  }

  private ObjectNode receiveResponse() throws IOException {
    int len = in.readInt();
    byte[] buf = new byte[len];
    in.readFully(buf);
    return (ObjectNode) mapper.readTree(buf);
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    try {
      ObjectNode command = mapper.createObjectNode();
      ObjectNode get = mapper.createObjectNode();
      get.put("key", key);
      command.set("Get", get);

      sendCommand(command);
      ObjectNode response = receiveResponse();

      if (response.has("Ok")) {
        ObjectNode ok = (ObjectNode) response.get("Ok");
        if (ok.has("Value")) {
          ObjectNode val = (ObjectNode) ok.get("Value");
          if (val.get("found").asBoolean(false)) {
            String value = val.get("value").asText();
            result.put("value", new StringByteIterator(value));
            return Status.OK;
          } else {
            return Status.NOT_FOUND;
          }
        }
      }

    } catch (IOException e) {
      e.printStackTrace();
      return Status.ERROR;
    }
    return Status.ERROR;
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    return setInternal(key, values);
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    return setInternal(key, values);
  }

  private Status setInternal(String key, Map<String, ByteIterator> values) {
    try {
      String value = values.get("value").toString();

      ObjectNode command = mapper.createObjectNode();
      ObjectNode set = mapper.createObjectNode();
      set.put("key", key);
      set.put("value", value);
      command.set("Set", set);

      sendCommand(command);
      ObjectNode response = receiveResponse();

      if (response.has("Ok") && response.get("Ok").has("SetResult")) {
        return Status.OK;
      } else {
        return Status.ERROR;
      }

    } catch (IOException e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(String table, String key) {
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, 
      Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  public void cleanup() throws DBException {
    try {
      socket.close();
    } catch (IOException e) {
      throw new DBException("Error closing connection", e);
    }
  }
}
