package com.yahoo.ycsb.db;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.policies.SpeculativeExecutionPolicy;
import com.google.common.base.Preconditions;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * This provides the execution plan that can dynamically update.
 */
class DynamicExecutionPlan implements SpeculativeExecutionPolicy.SpeculativeExecutionPlan {
  private final AtomicInteger remaining;
  private long dynamicDelay = 0L;

  DynamicExecutionPlan(final int maxSpeculativeExecutions) {
    this.remaining = new AtomicInteger(maxSpeculativeExecutions);
  }

  void setDynamicDelay(final long delay) {
    this.dynamicDelay = delay;
  }

  /**
   * @return the dynamicDelay
   */
  public long getDynamicDelay() {
    return dynamicDelay;
  }

  @Override
  public long nextExecution(Host lastQueried) {
    return (remaining.getAndDecrement() > 0) ? dynamicDelay : -1;
  }
}

/**
 * A {@link SpeculativeExecutionPolicy} that schedules a given number of speculative executions, separated by a dynamic
 * delay. Contains the dynamic plan such that it can be updated
 */
public class DynamicSpeculativeExecutionPolicy implements SpeculativeExecutionPolicy {
  private final int maxSpeculativeExecutions;
  private final DynamicExecutionPlan executionPlan;

  /**
   * Builds a new instance.
   *
   * @param maxSpeculativeExecutions the number of speculative executions. Must be strictly positive.
   * @throws IllegalArgumentException if one of the arguments does not respect the preconditions above.
   */
  DynamicSpeculativeExecutionPolicy(final int maxSpeculativeExecutions) {
    Preconditions.checkArgument(maxSpeculativeExecutions > 0,
        "number of speculative executions must be strictly positive (was %d)", maxSpeculativeExecutions);
    this.maxSpeculativeExecutions = maxSpeculativeExecutions;
    this.executionPlan = new DynamicExecutionPlan(maxSpeculativeExecutions);
  }

  public void setDynamicDelay(final long newDelay) {
    this.executionPlan.setDynamicDelay(newDelay);
  }

  public long getDynamicDelay() {
    return this.executionPlan.getDynamicDelay();
  }

  /**
   * This gets the already created plan. It does not depend on the stuff that can be added
   * as parameters here.
   */
  @Override
  public SpeculativeExecutionPlan newPlan(String loggedKeyspace, Statement statement) {
    return this.executionPlan;
  }

  @Override
  public void init(Cluster cluster) {
    // do nothing
  }

  @Override
  public void close() {
    // do nothing
  }
}

