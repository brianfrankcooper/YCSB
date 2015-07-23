package com.yahoo.ycsb.generator;

import java.util.concurrent.locks.ReentrantLock;

/**
 * A CounterGenerator that reports generated integers via lastInt()
 * only after they have been acknowledged.
 */
public class AcknowledgedCounterGenerator extends CounterGenerator
{
	private static final int WINDOW_SIZE = 10000;

	private ReentrantLock lock;
	private boolean[] window;
	private int limit;

	/**
	 * Create a counter that starts at countstart.
	 */
	public AcknowledgedCounterGenerator(int countstart)
	{
		super(countstart);
		lock = new ReentrantLock();
		window = new boolean[WINDOW_SIZE];
		limit = countstart - 1;
	}

	/**
	 * In this generator, the highest acknowledged counter value
	 * (as opposed to the highest generated counter value).
	 */
	@Override
	public int lastInt()
	{
		return limit;
	}

	/**
	 * Make a generated counter value available via lastInt().
	 */
	public void acknowledge(int value)
	{
		if (value > limit + WINDOW_SIZE) {
			throw new RuntimeException("This should be a different exception.");
		}

		window[value % WINDOW_SIZE] = true;

		if (lock.tryLock()) {
			// move a contiguous sequence from the window
			// over to the "limit" variable

			try {
				int index;

				for (index = limit + 1; index <= value; ++index) {
					int slot = index % WINDOW_SIZE;

					if (!window[slot]) {
						break;
					}

					window[slot] = false;
				}

				limit = index - 1;
			} finally {
				lock.unlock();
			}
		}
	}
}
