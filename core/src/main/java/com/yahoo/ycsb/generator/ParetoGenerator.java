package com.yahoo.ycsb.generator;

import com.yahoo.ycsb.Utils;

/*
 * According to the SIGMETRIC'12 paper:
 * "Workload Analysis of a Large-Scale Key-Value Store",
 * the value-size follows the generalized Pareto distribution.
 */

public class ParetoGenerator extends IntegerGenerator{
	public static final double theta = 0;
	public static final double sigma = 214.476;
	public static final double xi = 0.348238;
	
	/** 
	 * Generate the next item. this distribution will be skewed toward lower integers; e.g. 0 will
	 * be the most popular, 1 the next most popular, etc.
	 * @param itemcount The number of items in the distribution.
	 * @return The next item in the sequence.
	 */
	public int nextInt()
	{
		double u=Utils.random().nextDouble();
		return 1;
	}
	
	/**
	 * @todo Implement ZipfianGenerator.mean()
	 */
	@Override
	public double mean() {
		throw new UnsupportedOperationException("@todo implement ZipfianGenerator.mean()");
	}
}
