package com.yahoo.ycsb.webservice.rest;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.ServerSocket;

/**
 * Holds the common utility methods.
 * 
 * @author shivam.maharshi
 */
public class Utils {

	/**
	 * Returns true if the port is available.
	 * 
	 * @param port
	 * @return isAvailable
	 */
	public static boolean available(int port) {
		ServerSocket ss = null;
		DatagramSocket ds = null;
		try {
			ss = new ServerSocket(port);
			ss.setReuseAddress(true);
			ds = new DatagramSocket(port);
			ds.setReuseAddress(true);
			return true;
		} catch (IOException e) {
		} finally {
			if (ds != null) {
				ds.close();
			}
			if (ss != null) {
				try {
					ss.close();
				} catch (IOException e) {
					/* should not be thrown */
				}
			}
		}
		return false;
	}

}
