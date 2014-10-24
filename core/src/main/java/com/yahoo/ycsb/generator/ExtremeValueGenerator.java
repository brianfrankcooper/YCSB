package com.yahoo.ycsb.generator;

import com.yahoo.ycsb.Utils;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;

/*
 * According to the SIGMETRIC'12 paper:
 * "Workload Analysis of a Large-Scale Key-Value Store",
 * the key-size follows the generalized extreme value distribution.
 * 
 */
public class ExtremeValueGenerator extends IntegerGenerator{

	public static final double mu = 30.7984;
	public static final double sigma = 8.20449;
	public static final double xi = 0.078688;
	
	
	/** 
	 * Generate the next item. this distribution will be skewed toward lower integers; e.g. 0 will
	 * be the most popular, 1 the next most popular, etc.
	 * @param itemcount The number of items in the distribution.
	 * @return The next item in the sequence.
	 */
	public int nextInt()
	{
		double u=Utils.random().nextDouble();
		double ret = (mu + sigma*(Math.pow(-Math.log(u),-xi)-1)/xi);
		return (int)ret;
	}
	
	/**
	 * @todo Implement ZipfianGenerator.mean()
	 */
	@Override
	public double mean() {
		throw new UnsupportedOperationException("@todo implement ZipfianGenerator.mean()");
	}
	
	public static void main(String[] args)
	{
	       try {
	            System.setOut(new PrintStream(new FileOutputStream("system_out.txt")));

	        } catch (FileNotFoundException ex) {
	            ex.printStackTrace();
	            return;
	        }
	       
		ExtremeValueGenerator gen=new ExtremeValueGenerator();
		
		for (int i=0; i<1000000; i++)
		{
			System.out.println(""+gen.nextInt());
		}
	}
}
