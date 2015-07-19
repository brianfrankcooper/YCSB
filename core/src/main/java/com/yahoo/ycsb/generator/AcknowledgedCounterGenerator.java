package com.yahoo.ycsb.generator;

import java.util.PriorityQueue;

/**
 * A CounterGenerator that reports generated integers via lastInt()
 * only after they have been acknowledged.
 */
public class AcknowledgedCounterGenerator extends CounterGenerator
{
	private PriorityQueue<Integer> ack;
	private int limit;

	/**
	 * Create a counter that starts at countstart.
	 */
	public AcknowledgedCounterGenerator(int countstart)
	{
		super(countstart);
		ack = new PriorityQueue<Integer>();
		limit = countstart - 1;
	}

	@Override
	public int lastInt()
	{
		return limit;
	}

	public synchronized void acknowledge(int value)
	{
		ack.add(value);

		// move a contiguous sequence from the priority queue
		// over to the "limit" variable

		Integer min;

		while ((min = ack.peek()) != null && min == limit + 1) {
			limit = ack.poll();
		}
	}
}
