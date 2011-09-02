/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.yahoo.ycsb;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Enumeration;
import java.util.Properties;
import java.util.Vector;

import com.yahoo.ycsb.ZKCoordination;
import org.apache.zookeeper.KeeperException;

import com.yahoo.ycsb.measurements.Measurements;
import com.yahoo.ycsb.measurements.exporter.MeasurementsExporter;
import com.yahoo.ycsb.measurements.exporter.TextMeasurementsExporter;
import com.yahoo.ycsb.measurements.reporter.*;

public class MultiPhaseClient
{
	public static final String OPERATION_COUNT_PROPERTY="operationcount";

	public static final String RECORD_COUNT_PROPERTY="recordcount";

	public static final String WORKLOAD_PROPERTY="workload";

	public static final String PHASE_PROPERTY="phases";

	// Added for ZK-based barrier synchronization
	// 
	public static final String MULTICLIENT_BARRIER_PROPERTY="barrier-status";
	public static final String MULTICLIENT_BARRIER_DEFAULT="no";
	public static final String MULTICLIENT_BARRIER_ENABLED="yes";
	public static final String BARRIER_SERVER="barrier-zkServer";
	public static final String BARRIER_ROOT="barrier-zkRoot";
	public static final String BARRIER_SIZE="barrier-size";
	public static final String BARRIER_SIZE_DEFAULT="1";


	/**
	 * Indicates how many inserts to do, if less than recordcount. Useful for partitioning
	 * the load among multiple servers, if the client is the bottleneck. Additionally, workloads
	 * should support the "insertstart" property, which tells them which record to start at.
	 */
	public static final String INSERT_COUNT_PROPERTY="insertcount";

	public static final String LABEL_PROPERTY="label";

	private static final String STATUS_PROPERTY = "status";

	public static final String LOAD_PROPERTY = "load";
	public static final String LOAD_DEFAULT = "no";
	public static final String LOAD_ENABLED = "yes";
	

	private static MeasurementsExporter exporter = null;

	public static void usageMessage()
	{
		System.out.println("Usage: java com.yahoo.ycsb.Client [options]");
		System.out.println("Options:");
		System.out.println("  -threads n: execute using n threads (default: 1) - can also be specified as the \n" +
		"              \"threadcount\" property using -p");
		System.out.println("  -target n: attempt to do n operations per second (default: unlimited) - can also\n" +
		"             be specified as the \"target\" property using -p");
		System.out.println("  -load:  run the loading phase of the workload");
		System.out.println("  -t:  run the transactions phase of the workload (default)");
		System.out.println("  -db dbname: specify the name of the DB to use (default: com.yahoo.ycsb.BasicDB) - \n" +
		"              can also be specified as the \"db\" property using -p");
		System.out.println("  -P propertyfile: load properties from the given file. Multiple files can");
		System.out.println("                   be specified, and will be processed in the order specified");
		System.out.println("  -p name=value:  specify a property to be passed to the DB and workloads;");
		System.out.println("                  multiple properties can be specified, and override any");
		System.out.println("                  values in the propertyfile");
		System.out.println("  -s:  show status during run (default: no status)");
		System.out.println("  -l label:  use label for status (e.g. to label one experiment out of a whole batch)");
		System.out.println("");
		System.out.println("Required properties:");
		System.out.println("  "+WORKLOAD_PROPERTY+": the name of the workload class to use (e.g. com.yahoo.ycsb.workloads.CoreWorkload)");
		System.out.println("");
		System.out.println("To run the transaction phase from multiple servers, start a separate client on each.");
		System.out.println("To run the load phase from multiple servers, start a separate client on each; additionally,");
		System.out.println("use the \"insertcount\" and \"insertstart\" properties to divide up the records to be inserted");
	}

	public static boolean checkRequiredProperties(Properties props)
	{
		if (props.getProperty(WORKLOAD_PROPERTY)==null)
		{
			System.out.println("Missing property: "+WORKLOAD_PROPERTY);
			return false;
		}

		return true;
	}


	private static void initMeasurementsExporter(Properties props)
	{
		if (exporter == null) {
			try {
				// if no destination file is provided the results will be written to stdout
				OutputStream out;
				String exportFile = props.getProperty("exportfile");
				if (exportFile == null)
				{
					out = System.out;
				} else
				{
					out = new FileOutputStream(exportFile);
				}

				// if no exporter is provided the default text one will be used
				String exporterStr = props.getProperty("exporter", "com.yahoo.ycsb.measurements.exporter.TextMeasurementsExporter");
				try
				{
					exporter = (MeasurementsExporter) Class.forName(exporterStr).getConstructor(OutputStream.class).newInstance(out);
				} catch (Exception e)
				{
					System.err.println("Could not find exporter " + exporterStr
							+ ", will use default text reporter.");
					e.printStackTrace();
					exporter = new TextMeasurementsExporter(out);
				}
			} catch (IOException e) {
				exporter = null;
			}
		}
	}

	/**
	 * Exports the measurements to either sysout or a file using the exporter
	 * loaded from conf.
	 * @throws IOException Either failed to write to output stream or failed to close it.
	 */
	private static void exportMeasurements(Properties props, int opcount, long runtime)
	throws IOException
	{
		if (exporter != null) {
			exporter.write("OVERALL", "RunTime(ms)", runtime);
			double throughput = 1000.0 * ((double) opcount) / ((double) runtime);
			exporter.write("OVERALL", "Throughput(ops/sec)", throughput);
			Measurements.getMeasurements().exportMeasurements(exporter);
		}
	}

	private static void closeMeasurementsExporter(Properties props)
	{
                if  (props.getProperty("status") != null) {
                        String reportername = props.getProperty("reporter");
                        if (reportername != null) {
                            Reporter reporter = ReporterFactory.newReporter(reportername, props);
                            if (reporter != null) {
                                reporter.start();
                                reporter.send("ops", 0);
                                reporter.send("throughput", 0);
                                reporter.send("phase", 0);
                                reporter.commit();
                                reporter.end();
                            }
                        }
                }

		if (exporter != null) {
			try {
				exporter.close();
			} catch (IOException e) {
			}
		}
	}

	private static void runWorkload(Properties props)
	{
		boolean status=false;
		String dbname;
		int threadcount=1;
		int target=0;
		boolean dotransactions=true;
		String label=props.getProperty(LABEL_PROPERTY, "");

		if (!checkRequiredProperties(props))
		{
			System.exit(0);
		}

		if (props.getProperty("status") != null) {
			status = true;
		}

		//get number of threads, target and db
		threadcount=Integer.parseInt(props.getProperty("threadcount","1"));
		dbname=props.getProperty("db","com.yahoo.ycsb.BasicDB");
		target=Integer.parseInt(props.getProperty("target","0"));

		if (props.getProperty(LOAD_PROPERTY,LOAD_DEFAULT).compareTo(LOAD_ENABLED) == 0) {
			dotransactions=false;
			System.err.println("Not do transactions!");
		} else {
			System.err.println("Do transactions!");
		}

		//compute the target throughput
		double targetperthreadperms=-1;
		if (target>0)
		{
			double targetperthread=((double)target)/((double)threadcount);
			targetperthreadperms=targetperthread/1000.0;
		}	 


		//show a warning message that creating the workload is taking a while
		//but only do so if it is taking longer than 2 seconds 
		//(showing the message right away if the setup wasn't taking very long was confusing people)
		Thread warningthread=new Thread() 
		{
			public void run()
			{
				try
				{
					sleep(2000);
				}
				catch (InterruptedException e)
				{
					return;
				}
				System.err.println(" (might take a few minutes for large data sets)");
			}
		};

		warningthread.start();

		//set up measurements
		Measurements.setProperties(props);

		//load the workload
		ClassLoader classLoader = Client.class.getClassLoader();

		Workload workload=null;

		try 
		{
			Class workloadclass = classLoader.loadClass(props.getProperty(WORKLOAD_PROPERTY));

			workload=(Workload)workloadclass.newInstance();
		}
		catch (Exception e) 
		{  
			e.printStackTrace();
			e.printStackTrace(System.out);
			System.exit(0);
		}

		try
		{
			workload.init(props);
		}
		catch (WorkloadException e)
		{
			e.printStackTrace();
			e.printStackTrace(System.out);
			System.exit(0);
		}

		warningthread.interrupt();

		//run the workload

		System.err.println("Starting test.");

		int opcount;

		opcount=Integer.parseInt(props.getProperty(OPERATION_COUNT_PROPERTY,"0"));


		Vector<Thread> threads=new Vector<Thread>();

		for (int threadid=0; threadid<threadcount; threadid++)
		{
			DB db=null;
			try
			{
				db=DBFactory.newDB(dbname,props);
			}
			catch (UnknownDBException e)
			{
				System.out.println("Unknown DB "+dbname);
				System.exit(0);
			}

			Thread t=new ClientThread(db,dotransactions,workload,threadid,threadcount,props,opcount/threadcount,targetperthreadperms);

			threads.add(t);
			//t.start();
		}

		StatusThread statusthread=null;

		if (status)
		{
			boolean standardstatus=false;
			if (props.getProperty("measurementtype","").compareTo("timeseries")==0) 
			{
				standardstatus=true;
			}
			String phase=props.getProperty(PHASE_PROPERTY, "0");
			statusthread=new StatusThread(threads,label,phase,standardstatus, props);
			statusthread.start();
		}

		long st=System.currentTimeMillis();

		for (Thread t : threads)
		{
			t.start();
		}

		for (Thread t : threads)
		{
			try
			{
				t.join();
			}
			catch (InterruptedException e)
			{
			}
		}

		long en=System.currentTimeMillis();

		if (status)
		{
			statusthread.interrupt();
		}

		try
		{
			workload.cleanup();
		}
		catch (WorkloadException e)
		{
			e.printStackTrace();
			e.printStackTrace(System.out);
			System.exit(0);
		}

		try
		{
			exportMeasurements(props, opcount, en - st);
		} catch (IOException e)
		{
			System.err.println("Could not export measurements, error: " + e.getMessage());
			e.printStackTrace();
			System.exit(-1);
		}

	}

	@SuppressWarnings("unchecked")
	public static void main(String[] args)
	{

		Properties props=new Properties();
		//Properties fileprops=new Properties();
		int phases;

		//parse arguments
		int argindex=0;

		if (args.length==0)
		{
			usageMessage();
			System.exit(0);
		}

		while (args[argindex].startsWith("-"))
		{
			if (args[argindex].compareTo("-threads")==0)
			{
				argindex++;
				if (argindex>=args.length)
				{
					usageMessage();
					System.exit(0);
				}
				int tcount=Integer.parseInt(args[argindex]);
				props.setProperty("threadcount", tcount+"");
				argindex++;
			}
			else if (args[argindex].compareTo("-target")==0)
			{
				argindex++;
				if (argindex>=args.length)
				{
					usageMessage();
					System.exit(0);
				}
				int ttarget=Integer.parseInt(args[argindex]);
				props.setProperty("target", ttarget+"");
				argindex++;
			}
			else if (args[argindex].compareTo("-s")==0)
			{
				props.setProperty(STATUS_PROPERTY, "true");
				argindex++;
			}
			else if (args[argindex].compareTo("-db")==0)
			{
				argindex++;
				if (argindex>=args.length)
				{
					usageMessage();
					System.exit(0);
				}
				props.setProperty("db",args[argindex]);
				argindex++;
			}
			else if (args[argindex].compareTo("-l")==0)
			{
				argindex++;
				if (argindex>=args.length)
				{
					usageMessage();
					System.exit(0);
				}
				props.setProperty(LABEL_PROPERTY, args[argindex]);
				argindex++;
			}
			else if (args[argindex].compareTo("-load")==0)
			{
				props.setProperty(LOAD_PROPERTY, "yes");
				argindex++;
			}
			else if (args[argindex].compareTo("-t")==0)
			{
				props.setProperty(LOAD_PROPERTY, "no");
				argindex++;
			}
			else if (args[argindex].compareTo("-P")==0)
			{
				argindex++;
				if (argindex>=args.length)
				{
					usageMessage();
					System.exit(0);
				}
				String propfile=args[argindex];
				argindex++;

				Properties myfileprops=new Properties();
				try
				{
					myfileprops.load(new FileInputStream(propfile));
				}
				catch (IOException e)
				{
					System.out.println(e.getMessage());
					System.exit(0);
				}

				//Issue #5 - remove call to stringPropertyNames to make compilable under Java 1.5
				for (Enumeration e=myfileprops.propertyNames(); e.hasMoreElements(); )
				{
					String prop=(String)e.nextElement();

					props.setProperty(prop,myfileprops.getProperty(prop));
				}

			}
			else if (args[argindex].compareTo("-p")==0)
			{
				argindex++;
				if (argindex>=args.length)
				{
					usageMessage();
					System.exit(0);
				}
				int eq=args[argindex].indexOf('=');
				if (eq<0)
				{
					usageMessage();
					System.exit(0);
				}

				String name=args[argindex].substring(0,eq);
				String value=args[argindex].substring(eq+1);
				props.put(name,value);
				//System.out.println("["+name+"]=["+value+"]");
				argindex++;
			}
			else
			{
				System.out.println("Unknown option "+args[argindex]);
				usageMessage();
				System.exit(0);
			}

			if (argindex>=args.length)
			{
				break;
			}
		}

		if (argindex!=args.length)
		{
			usageMessage();
			System.exit(0);
		}

		//set up logging
		//BasicConfigurator.configure();

		//overwrite file properties with properties from the command line

		//Issue #5 - remove call to stringPropertyNames to make compilable under Java 1.5
		/*for (Enumeration e=props.propertyNames(); e.hasMoreElements(); )
		{
			String prop=(String)e.nextElement();

			fileprops.setProperty(prop,props.getProperty(prop));
		}

		props=fileprops;
*/


		initMeasurementsExporter(props);

		//Check of barrier-synchronization property
		String address = "";
		String root = "";
		int size = 0;
		ZKCoordination.Barrier zk_barrier = null;
		String barrier_flag = "";

		barrier_flag = props.getProperty(MULTICLIENT_BARRIER_PROPERTY, MULTICLIENT_BARRIER_DEFAULT).trim();
		if (barrier_flag.equals(MULTICLIENT_BARRIER_ENABLED)) {
			System.out.println("*** BarrierStatus=" + barrier_flag);
			address = props.getProperty(BARRIER_SERVER);
      if (address == null)
          throw new RuntimeException("Error in config file: barrier paramter ["+BARRIER_SERVER+"] missing"); 
			root = props.getProperty(BARRIER_ROOT);
      if (root == null)
          throw new RuntimeException("Error in config file: barrier paramter ["+BARRIER_ROOT+"] missing"); 
			size = Integer.parseInt(props.getProperty(BARRIER_SIZE, BARRIER_SIZE_DEFAULT));
			System.out.println("*** BarrierInfo(zkserver="+address+";root="+root+"barriersize="+size+")");
		}

		initMeasurementsExporter(props);


		System.out.println("YCSB Client 0.1");
		System.out.print("Command line:");
		for (int i=0; i<args.length; i++)
		{
			System.out.print(" "+args[i]);
		}
		System.out.println();
		System.err.println("Loading workload...");


		phases = Integer.parseInt(props.getProperty(PHASE_PROPERTY,"1"));
		System.out.println("There are " + phases + " phases to run in total.");
		props.remove(PHASE_PROPERTY);
		int i;
		for (i = 1;i <= phases; i ++)
		{
			System.out.println("Start to run phase " + i + " at time " + System.currentTimeMillis());
			Properties currentProps=new Properties();
			currentProps.setProperty(PHASE_PROPERTY, Integer.toString(i));
			for (Enumeration e=props.propertyNames(); e.hasMoreElements(); )
			{
				String prop=(String)e.nextElement();

				System.err.println(prop + " " + props.getProperty(prop));



				if (!prop.startsWith("phase")) {//General option independent of phases
					if (currentProps.getProperty(prop) == null)
						currentProps.setProperty(prop, props.getProperty(prop));
				} else if (prop.startsWith("phase"+i)){//phase specific option
					// The property is set by phase1.prop=some
					// So we need to remove "phase1." to form prop=some
					currentProps.setProperty(prop.substring(("phase"+i).length() + 1), props.getProperty(prop));

				}
			}
			System.err.println();
			for (Enumeration e=currentProps.propertyNames(); e.hasMoreElements(); )
			{
				String prop=(String)e.nextElement();
				System.err.println(prop + " " + currentProps.getProperty(prop));
			}

			//Check of barrier-synchronization property
			String phase_address = "";
			String phase_root = "";
			int phase_size = 0;
			String phase_barrier_flag = "";

			phase_barrier_flag = currentProps.getProperty(MULTICLIENT_BARRIER_PROPERTY, MULTICLIENT_BARRIER_DEFAULT).trim();
			if (phase_barrier_flag.equals(MULTICLIENT_BARRIER_ENABLED)) {
				System.out.println("*** BarrierStatus=" + phase_barrier_flag);
				phase_address = currentProps.getProperty(BARRIER_SERVER);
				if (phase_address == null)
					phase_address = address;
				if (currentProps.getProperty(BARRIER_ROOT) == null)
					phase_root = "/" + root + "-" + i;
				else
					phase_root = currentProps.getProperty(BARRIER_ROOT)+"-"+i;
				if (currentProps.getProperty(BARRIER_SIZE) == null)
					phase_size = size;
				else
					phase_size = Integer.parseInt(currentProps.getProperty(BARRIER_SIZE));
				System.out.println("*** BarrierInfo(zkserver="+phase_address+";root="+phase_root+"barriersize="+phase_size+")");
				zk_barrier = new ZKCoordination.Barrier(phase_address, phase_root, phase_size);
				enter_barrier(zk_barrier);
			}

			if (phase_barrier_flag.equals(MULTICLIENT_BARRIER_DEFAULT) && 
					barrier_flag.equals(MULTICLIENT_BARRIER_ENABLED)) {
				zk_barrier = new ZKCoordination.Barrier(address, root, size);
				enter_barrier(zk_barrier);
			}

			runWorkload(currentProps);

			System.out.println("Phase " + i + " done at time " + System.currentTimeMillis());

			Measurements.getMeasurements().cleanMeasurement();


			if (phase_barrier_flag.equals(MULTICLIENT_BARRIER_ENABLED) ||
					barrier_flag.equals(MULTICLIENT_BARRIER_ENABLED)) {
				leave_barrier(zk_barrier);
			}

			Measurements.getMeasurements().cleanMeasurement();

		}

		closeMeasurementsExporter(props);

		System.exit(0);
	}

	private static void enter_barrier(ZKCoordination.Barrier zk_bar)
	{
		System.out.println("*** enter_barrier()");
		try {
			boolean ret_val = zk_bar.enter();
			if (ret_val == false)
				System.out.println("*** entering barrier");
			else
				System.out.println("*** not entering barrier");
		} catch (KeeperException e) {
			System.out.println("*** enter_barrier() keeper exception:"+e.toString());
		} catch (InterruptedException e) {
			System.out.println("*** enter_barrier() intr exception:"+e.toString());
		} catch (IOException e) {
                }
	}

	private static void leave_barrier(ZKCoordination.Barrier zk_bar)
	{
		System.out.println("*** leaveBarrier()");
		try {
			boolean ret_val = zk_bar.leave();
			if (ret_val == false)
				System.out.println("*** leaving barrier");
			else
				System.out.println("*** not leaving barrier");
		} catch (KeeperException e) {
			System.out.println("*** leave _barrier() keeper exception:"+e.toString());
		} catch (InterruptedException e) {
			System.out.println("*** leave_barrier() intr exception:"+e.toString());
		} catch (IOException e) {
                }
	}

}
