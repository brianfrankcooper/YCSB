package com.yahoo.ycsb.generator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

public class UniqueRandomGenerator extends IntegerGenerator
{
	private ArrayList<Integer> items;
	private AtomicInteger counter;
	private int size; // XXX: ArrayList doesn't get() long's, will this cause problems?

	public UniqueRandomGenerator(long count)
	{
		this.size = (int)count;
		items = new ArrayList<Integer>(this.size);
		for(int i = 0; i < this.size; i++) {
		    items.add(i);
		}
		Collections.shuffle(items);
		counter = new AtomicInteger(0);
	}

	@Override
	public long nextInt()
	{
		int c = counter.getAndIncrement();
		setLastInt(c);
		return items.get(c % size);
	}

	@Override
	public double mean() {
		throw new UnsupportedOperationException("Can't compute mean");
	}
}
