package com.yahoo.ycsb.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR;

/**
 * Utility class to create Apache Phoenix http://phoenix.apache.org/ table for YCSB
 * 
 * Optional arguments for create table
 * 
 * -c <compression>   On disk compression. GZ, SNAPPY, etc. Default: none
 * -f <fieldcount>    Field count. Default: 10
 * -h,--help          Print help information
 * -m <multifamily>   Multi Column Family table. Default: false (Single Column Family)
 * -s <saltbuckets>   Number of salt buckets. Default: 0 (no salting)
 * -t <tablename>     Table name. Default: usertable
 * -z <zookeeper>     HBase Zookeeper. Default: localhost
 * 
 * Usage example: java -jar phoenix/target/phoenix-binding-0.1.4.jar -z myserver -s 10
 * 
 * @author mchohan
 *
 */
public class PhoenixCreateTable implements PhoenixConstants {

	/**
	 * See class comments for usage instructions
	 * 
	 * @param args
	 */
	@SuppressWarnings("static-access")
	public static void main(String[] args) {

		Options options = new Options();

		options.addOption(OptionBuilder.withValueSeparator().hasArgs()
				.withArgName("zookeeper")
				.withDescription("HBase Zookeeper. Default: localhost")
				.create('z'));

		options.addOption(OptionBuilder
				.withValueSeparator()
				.hasArgs()
				.withArgName("saltbuckets")
				.withDescription(
						"Number of salt buckets. Default: 0 (no salting)")
				.create('s'));

		options.addOption(OptionBuilder
				.withValueSeparator()
				.hasArgs()
				.withArgName("compression")
				.withDescription(
						"On disk compression. GZ, SNAPPY, etc. Default: none")
				.create('c'));

		options.addOption(OptionBuilder
				.withValueSeparator()
				.hasArgs()
				.withArgName("multifamily")
				.withDescription(
						"Multi Column Family table. Default: false (Single Column Family)")
				.create('m'));

		options.addOption(OptionBuilder.withValueSeparator().hasArgs()
				.withArgName("fieldcount")
				.withDescription("Field count. Default: 10").create('f'));

		options.addOption(OptionBuilder.withValueSeparator().hasArgs()
				.withArgName("tablename")
				.withDescription("Table name. Default: usertable").create('t'));

		options.addOption(new Option("h", "help", false,
				"Print help information"));

		CommandLineParser parser = new GnuParser();
		try {
			CommandLine cline = parser.parse(options, args);
			String zookeeper = cline.getOptionValue("z", "localhost");
			int saltBuckets = Integer.parseInt(cline.getOptionValue("s", "0"));
			String compression = cline.getOptionValue("c", "");
			boolean multiFamily = Boolean.parseBoolean(cline.getOptionValue(
					"m", "false"));
			int fieldcount = Integer.parseInt(cline.getOptionValue("f", "10"));
			String tablename = cline.getOptionValue("t", "usertable");

			if (cline.hasOption('h')) {
				printHelp(options);
				return;
			}

			createTable(zookeeper, tablename, fieldcount, saltBuckets,
					compression, multiFamily);

		} catch (ParseException exp) {
			System.err.println("Parsing failed.  Reason: " + exp.getMessage());
			printHelp(options);
		}
	}

	/**
	 * Print help on screen
	 * 
	 * @param options
	 */
	private static void printHelp(Options options) {
		new HelpFormatter().printHelp("PhoenixCreateTable", options);
	}

	/**
	 * 
	 * @param zookeeper
	 * @param tableName
	 * @param fieldCount
	 * @param saltBuckets
	 * @param compression
	 * @param multiFamily
	 */
	private static void createTable(String zookeeper, String tableName,
			int fieldCount, int saltBuckets, String compression,
			boolean multiFamily) {
		Statement stmt = null;
		try {
			Properties info = new Properties();
			Connection connection = DriverManager.getConnection(JDBC_PROTOCOL
					+ JDBC_PROTOCOL_SEPARATOR + zookeeper, info);

			stmt = connection.createStatement();
			stmt.execute("DROP TABLE IF EXISTS " + tableName);

			StringBuilder sql = new StringBuilder("CREATE TABLE ");
			sql.append(tableName);
			sql.append(" (");
			sql.append(PRIMARY_KEY);
			sql.append(" VARCHAR PRIMARY KEY");

			for (int idx = 0; idx < fieldCount; idx++) {
				sql.append(", ");
				if (multiFamily) {
					sql.append("CF");
					sql.append(idx);
					sql.append(".");
				}
				sql.append("FIELD");
				sql.append(idx);
				sql.append(" VARCHAR");
			}
			sql.append(") ");

			if (saltBuckets != 0) {
				sql.append(" SALT_BUCKETS=");
				sql.append(saltBuckets);
			}

			if (!compression.isEmpty()) {
				if (saltBuckets != 0)
					sql.append(",");
				sql.append("COMPRESSION='");
				sql.append(compression);
				sql.append("'");
			}

			stmt.execute(sql.toString());
			System.out
					.println("Phoenix table created successfully: Table name: "
							+ tableName);
			stmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (stmt != null)
				try {
					stmt.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
		}
	}
}
