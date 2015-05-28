package com.yahoo.ycsb.generator;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;

import com.yahoo.ycsb.Utils;

class GeneralizedParetoGenerator extends IntegerGenerator{
	private static final double loc = 15; // location
	private static final double scale = 214.476; // scale
	private static final double shape = 0.348238; //shape
	
	@Override
	public int nextInt() {
		double u = Utils.random().nextDouble();
		double ret = loc + scale *(Math.pow(u, -shape) -1)/shape;
		return (int)ret;
	}

	@Override
	public double mean() {
		// TODO Auto-generated method stub
		return 0;
	}
	
}

public class FacebookValueGenerator extends IntegerGenerator {

	/* discrete distribution */
	double[] hist;
	GeneralizedParetoGenerator gpg;
	
	public FacebookValueGenerator(){
		gpg = new GeneralizedParetoGenerator();
		hist = new double[15];
		hist[0] = 0.00536;
		hist[1] = 0.00047;
		hist[2] = 0.17820;
		hist[3] = 0.09239;
		hist[4] = 0.00018;
		hist[5] = 0.02740;
		hist[6] = 0.00065;
		hist[7] = 0.00606;
		hist[8] = 0.00023;
		hist[9] = 0.00837;
		hist[10] = 0.00837;
		hist[11] = 0.08989;
		hist[12] = 0.00092;
		hist[13] = 0.00326;
		hist[14] = 0.01980;
	}
	
	@Override
	public int nextInt() {
		double u = Utils.random().nextDouble();
		double sum = 0;
		for(int i = 0; i<hist.length; i++){
			sum += hist[i];
			if(sum>u) return i;
		}
		
		return gpg.nextInt();
	}

	@Override
	public double mean() {
		throw new UnsupportedOperationException(
				"@todo implement ZipfianGenerator.mean()");
	}
	
	public static void main(String[] args){
		try {
			System.setOut(new PrintStream(
					new FileOutputStream("system_out.txt")));

		} catch (FileNotFoundException ex) {
			ex.printStackTrace();
			return;
		}

		FacebookValueGenerator gen = new FacebookValueGenerator();

		for (int i = 0; i < 1000000; i++) {
			System.out.println("" + gen.nextInt());
		}
		
	}

}
