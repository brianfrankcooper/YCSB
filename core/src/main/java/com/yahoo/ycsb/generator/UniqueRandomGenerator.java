package com.yahoo.ycsb.generator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

public class UniqueRandomGenerator extends IntegerGenerator
{
	private ArrayList<Integer> items;
    private AtomicInteger counter;
    private int size;

	public UniqueRandomGenerator(int count)
	{
        this.size = count;
		items = new ArrayList<Integer>(count);
        for(int i = 0; i < count; i++) {
            items.add(i);
        }
        Collections.shuffle(items);
        counter = new AtomicInteger(0);
	}

    @Override
    public int nextInt()
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
