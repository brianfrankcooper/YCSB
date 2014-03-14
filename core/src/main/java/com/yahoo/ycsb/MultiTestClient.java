package com.yahoo.ycsb;

import com.yahoo.ycsb.db.GigaSpacesClient;

public class MultiTestClient {

	public static void main(String[] args) {
		Client.main(args);
		String nextTestArgs[] = new String [args.length-1];
		int counter=0;
		for (int i = 0; i < args.length; i++) {
			String string = args[i];
			if (string.equalsIgnoreCase("-load"))
			{
				continue;
			}
			else
			{
				nextTestArgs[counter]=string;
				counter++;
			}
		}
		GigaSpacesClient.reset();
		Client.main(nextTestArgs);
		System.exit(0);
	}

}
