package com.yahoo.ycsb.db;

import com.datastax.driver.core.*;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.Map;

/**
 * MetaSession which contains multiple sessions and a performance state.
 */
public class MetaSession implements Session {
  // These should be implemented
  @Override
  public ResultSet execute(String query) {
    return null;
  }

  @Override
  public ResultSet execute(String query, Object... values) {
    return null;
  }

  @Override
  public ResultSet execute(String query, Map<String, Object> values) {
    return null;
  }

  @Override
  public ResultSet execute(Statement statement) {
    return null;
  }

  @Override
  public PreparedStatement prepare(String query) {
    return null;
  }

  @Override
  public PreparedStatement prepare(RegularStatement statement) {
    return null;
  }

  @Override
  public void close() {

  }

  // These could be ignored.
  @Override
  public String getLoggedKeyspace() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Session init() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ListenableFuture<Session> initAsync() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ResultSetFuture executeAsync(String query) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ResultSetFuture executeAsync(String query, Object... values) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ResultSetFuture executeAsync(String query, Map<String, Object> values) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ResultSetFuture executeAsync(Statement statement) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ListenableFuture<PreparedStatement> prepareAsync(String query) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ListenableFuture<PreparedStatement> prepareAsync(RegularStatement statement) {
    throw new UnsupportedOperationException();
  }

  @Override
  public CloseFuture closeAsync() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isClosed() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Cluster getCluster() {
    throw new UnsupportedOperationException();
  }

  @Override
  public State getState() {
    throw new UnsupportedOperationException();
  }
}
