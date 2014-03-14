package com.yahoo.ycsb.db;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Arrays;
import java.util.HashMap;

public class OutputAnalyzer {

	public static void main(String[] args) throws Exception {
		String outputFolder = args[0];
		File directory = new File(outputFolder);
		File[] myarray;
		String operations[] = { "OVERALL" , "UPDATE", "READ", "INSERT" };
		String stats[] = { "AverageLatency", "MinLatency", "MaxLatency","Throughput" };
		myarray = new File[100];
		myarray = directory.listFiles();

		HashMap<String, HashMap<String, String>> results = new HashMap<String, HashMap<String, String>>();
		for (int j = 0; j < myarray.length; j++) {
			String operation = "";
			File path = myarray[j];
			FileReader fr = new FileReader(path);
			BufferedReader br = new BufferedReader(fr);
			while (br.ready()) {
				String line = br.readLine();
				
				// looking for a matching stat
				for (int s = 0; s < stats.length; s++) {
					if (line.indexOf(stats[s]) > -1) {
						
						// looking for a matching operation
						for (int i = 0; i < operations.length; i++) {
							if (line.indexOf(operations[i]) > -1) {
								operation = operations[i];
								String statvalue = line.substring(
										line.indexOf(stats[s])
												+ stats[s].length() + 5,
										line.length());

								HashMap<String, String> statsMap = null;
								if (!results.containsKey(path.getName())) {
									statsMap = new HashMap<String, String>();
								} else {
									statsMap = results.get(path.getName());
								}
								statsMap.put(operation + " " + stats[s],
										statvalue.trim());
								results.put(path.getName(), statsMap);
							}
						}
					}
				}
			}
		}
//		System.out.println(results);
		
		String files[]=new String[results.size()];
		files = results.keySet().toArray(files);
		Arrays.sort(files);

		for (int o = 0; o < operations.length; o++) {
			System.out.println(operations[o]);
			for (int s = 0; s < stats.length; s ++) {
				for (int f = 0; f < files.length; f++) {
					String fileName = files[f];
					HashMap<String, String> stats_ = results.get(fileName);
					if (stats_.containsKey(operations[o] + " " + stats[s]))
						System.out.println(fileName + " " + operations[o] + " " + stats[s] + " " +stats_.get(operations[o] + " " + stats[s]));
				}
			}			
		}
	}
}
