/**                                                                                                                                                                                
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.                                                                                                                             
 *                                                                                                                                                                                 
 * Licensed under the Apache License, Version 2.0 (the "License"); you                                                                                                             
 * may not use this file except in compliance with the License. You                                                                                                                
 * may obtain a copy of the License at                                                                                                                                             
 *                                                                                                                                                                                 
 * http://www.apache.org/licenses/LICENSE-2.0                                                                                                                                      
 *                                                                                                                                                                                 
 * Unless required by applicable law or agreed to in writing, software                                                                                                             
 * distributed under the License is distributed on an "AS IS" BASIS,                                                                                                               
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or                                                                                                                 
 * implied. See the License for the specific language governing                                                                                                                    
 * permissions and limitations under the License. See accompanying                                                                                                                 
 * LICENSE file.                                                                                                                                                                   
 */

package com.yahoo.ycsb;


import java.io.*;
import java.text.DecimalFormat;
import java.util.*;

import com.yahoo.ycsb.measurements.Measurements;
import com.yahoo.ycsb.measurements.exporter.MeasurementsExporter;
import com.yahoo.ycsb.measurements.exporter.TextMeasurementsExporter;

//import org.apache.log4j.BasicConfigurator;

/**
 * A thread to periodically show the status of the experiment, to reassure you that progress is being made.
 * 
 * @author cooperb
 *
 */
class StatusThread extends Thread
{
	Vector<Thread> _threads;
	String _label;
	boolean _standardstatus;
	
	/**
	 * The interval for reporting status.
	 */
	public static final long sleeptime=10000;

	public StatusThread(Vector<Thread> threads, String label, boolean standardstatus)
	{
		_threads=threads;
		_label=label;
		_standardstatus=standardstatus;
	}

	/**
	 * Run and periodically report status.
	 */
	public void run()
	{
		long st=System.currentTimeMillis();

		long lasten=st;
		long lasttotalops=0;
		
		boolean alldone;

		do 
		{
			alldone=true;

			int totalops=0;

			//terminate this thread when all the worker threads are done
			for (Thread t : _threads)
			{
				if (t.getState()!=Thread.State.TERMINATED)
				{
					alldone=false;
				}

				ClientThread ct=(ClientThread)t;
				totalops+=ct.getOpsDone();
			}

			long en=System.currentTimeMillis();

			long interval=en-st;
			//double throughput=1000.0*((double)totalops)/((double)interval);

			double curthroughput=1000.0*(((double)(totalops-lasttotalops))/((double)(en-lasten)));
			
			lasttotalops=totalops;
			lasten=en;
			
			DecimalFormat d = new DecimalFormat("#.##");
			
			if (totalops==0)
			{
				System.err.println(_label+" "+(interval/1000)+" sec: "+totalops+" operations; "+Measurements.getMeasurements().getSummary());
			}
			else
			{
				System.err.println(_label+" "+(interval/1000)+" sec: "+totalops+" operations; "+d.format(curthroughput)+" current ops/sec; "+Measurements.getMeasurements().getSummary());
			}

			if (_standardstatus)
			{
			if (totalops==0)
			{
				System.out.println(_label+" "+(interval/1000)+" sec: "+totalops+" operations; "+Measurements.getMeasurements().getSummary());
			}
			else
			{
				System.out.println(_label+" "+(interval/1000)+" sec: "+totalops+" operations; "+d.format(curthroughput)+" current ops/sec; "+Measurements.getMeasurements().getSummary());
			}
			}

			try
			{
				sleep(sleeptime);
			}
			catch (InterruptedException e)
			{
				//do nothing
			}

		}
		while (!alldone);
	}
}

/**
 * A thread for executing transactions or data inserts to the database.
 * 
 * @author cooperb
 *
 */
class ClientThread extends Thread
{
	DB _db;
	boolean _dotransactions;
	Workload _workload;
	int _opcount;
	double _target;

	int _opsdone;
	int _threadid;
	int _threadcount;
	Object _workloadstate;
	Properties _props;


	/**
	 * Constructor.
	 * 
	 * @param db the DB implementation to use
	 * @param dotransactions true to do transactions, false to insert data
	 * @param workload the workload to use
	 * @param threadid the id of this thread 
	 * @param threadcount the total number of threads 
	 * @param props the properties defining the experiment
	 * @param opcount the number of operations (transactions or inserts) to do
	 * @param targetperthreadperms target number of operations per thread per ms
	 */
	public ClientThread(DB db, boolean dotransactions, Workload workload, int threadid, int threadcount, Properties props, int opcount, double targetperthreadperms)
	{
		//TODO: consider removing threadcount and threadid
		_db=db;
		_dotransactions=dotransactions;
		_workload=workload;
		_opcount=opcount;
		_opsdone=0;
		_target=targetperthreadperms;
		_threadid=threadid;
		_threadcount=threadcount;
		_props=props;
		//System.out.println("Interval = "+interval);
	}

	public int getOpsDone()
	{
		return _opsdone;
	}

	public void run()
	{
		try
		{
			_db.init();
		}
		catch (DBException e)
		{
			e.printStackTrace();
			e.printStackTrace(System.out);
			return;
		}

		try
		{
			_workloadstate=_workload.initThread(_props,_threadid,_threadcount);
		}
		catch (WorkloadException e)
		{
			e.printStackTrace();
			e.printStackTrace(System.out);
			return;
		}

		//spread the thread operations out so they don't all hit the DB at the same time
		try
		{
		   //GH issue 4 - throws exception if _target>1 because random.nextInt argument must be >0
		   //and the sleep() doesn't make sense for granularities < 1 ms anyway
		   if ( (_target>0) && (_target<=1.0) ) 
		   {
		      sleep(Utils.random().nextInt((int)(1.0/_target)));
		   }
		}
		catch (InterruptedException e)
		{
		  // do nothing.
		}
		
		try
		{
			if (_dotransactions)
			{
				long st=System.currentTimeMillis();

				while (((_opcount == 0) || (_opsdone < _opcount)) && !_workload.isStopRequested())
				{

					if (!_workload.doTransaction(_db,_workloadstate))
					{
						break;
					}

					_opsdone++;

					//throttle the operations
					if (_target>0)
					{
						//this is more accurate than other throttling approaches we have tried,
						//like sleeping for (1/target throughput)-operation latency,
						//because it smooths timing inaccuracies (from sleep() taking an int, 
						//current time in millis) over many operations
						while (System.currentTimeMillis()-st<((double)_opsdone)/_target)
						{
							try
							{
								sleep(1);
							}
							catch (InterruptedException e)
							{
							  // do nothing.
							}

						}
					}
				}
			}
			else
			{
				long st=System.currentTimeMillis();

				while (((_opcount == 0) || (_opsdone < _opcount)) && !_workload.isStopRequested())
				{

					if (!_workload.doInsert(_db,_workloadstate))
					{
						break;
					}

					_opsdone++;

					//throttle the operations
					if (_target>0)
					{
						//this is more accurate than other throttling approaches we have tried,
						//like sleeping for (1/target throughput)-operation latency,
						//because it smooths timing inaccuracies (from sleep() taking an int, 
						//current time in millis) over many operations
						while (System.currentTimeMillis()-st<((double)_opsdone)/_target)
						{
							try 
							{
								sleep(1);
							}
							catch (InterruptedException e)
							{
							  // do nothing.
							}
						}
					}
				}
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
			e.printStackTrace(System.out);
			System.exit(0);
		}

		try
		{
			_db.cleanup();
		}
		catch (DBException e)
		{
			e.printStackTrace();
			e.printStackTrace(System.out);
			return;
		}
	}
}

/**
 * Main class for executing YCSB.
 */
public class Client
{

	public static final String OPERATION_COUNT_PROPERTY="operationcount";

	public static final String RECORD_COUNT_PROPERTY="recordcount";

	public static final String WORKLOAD_PROPERTY="workload";
	
	/**
	 * Indicates how many inserts to do, if less than recordcount. Useful for partitioning
	 * the load among multiple servers, if the client is the bottleneck. Additionally, workloads
	 * should support the "insertstart" property, which tells them which record to start at.
	 */
	public static final String INSERT_COUNT_PROPERTY="insertcount";
	
	/**
   * The maximum amount of time (in seconds) for which the benchmark will be run.
   */
  public static final String MAX_EXECUTION_TIME = "maxexecutiontime";

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


	/**
	 * Exports the measurements to either sysout or a file using the exporter
	 * loaded from conf.
	 * @throws IOException Either failed to write to output stream or failed to close it.
	 */
	private static void exportMeasurements(Properties props, int opcount, long runtime)
			throws IOException
	{
		MeasurementsExporter exporter = null;
		try
		{
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

			exporter.write("OVERALL", "RunTime(ms)", runtime);
			double throughput = 1000.0 * ((double) opcount) / ((double) runtime);
			exporter.write("OVERALL", "Throughput(ops/sec)", throughput);

			Measurements.getMeasurements().exportMeasurements(exporter);
		} finally
		{
			if (exporter != null)
			{
				exporter.close();
			}
		}
	}
	
	@SuppressWarnings("unchecked")
	public static void main(String[] args)
	{
		String dbname;
		Properties props=new Properties();
		Properties fileprops=new Properties();
		boolean dotransactions=true;
		int threadcount=1;
		int target=0;
		boolean status=false;
		String label="";

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
			else if (args[argindex].compareTo("-load")==0)
			{
				dotransactions=false;
				argindex++;
			}
			else if (args[argindex].compareTo("-t")==0)
			{
				dotransactions=true;
				argindex++;
			}
			else if (args[argindex].compareTo("-s")==0)
			{
				status=true;
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
				label=args[argindex];
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
				   
				   fileprops.setProperty(prop,myfileprops.getProperty(prop));
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
		for (Enumeration e=props.propertyNames(); e.hasMoreElements(); )
		{
		   String prop=(String)e.nextElement();
		   
		   fileprops.setProperty(prop,props.getProperty(prop));
		}

		props=fileprops;

		if (!checkRequiredProperties(props))
		{
			System.exit(0);
		}
		
		long maxExecutionTime = Integer.parseInt(props.getProperty(MAX_EXECUTION_TIME, "0"));

		//get number of threads, target and db
		threadcount=Integer.parseInt(props.getProperty("threadcount","1"));
		dbname=props.getProperty("db","com.yahoo.ycsb.BasicDB");
		target=Integer.parseInt(props.getProperty("target","0"));
		
		//compute the target throughput
		double targetperthreadperms=-1;
		if (target>0)
		{
			double targetperthread=((double)target)/((double)threadcount);
			targetperthreadperms=targetperthread/1000.0;
		}	 

		System.out.println("YCSB Client 0.1");
		System.out.print("Command line:");
		for (int i=0; i<args.length; i++)
		{
			System.out.print(" "+args[i]);
		}
		System.out.println();
		System.err.println("Loading workload...");
		
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
		if (dotransactions)
		{
			opcount=Integer.parseInt(props.getProperty(OPERATION_COUNT_PROPERTY,"0"));
		}
		else
		{
			if (props.containsKey(INSERT_COUNT_PROPERTY))
			{
				opcount=Integer.parseInt(props.getProperty(INSERT_COUNT_PROPERTY,"0"));
			}
			else
			{
				opcount=Integer.parseInt(props.getProperty(RECORD_COUNT_PROPERTY,"0"));
			}
		}

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
			statusthread=new StatusThread(threads,label,standardstatus);
			statusthread.start();
		}

		long st=System.currentTimeMillis();

		for (Thread t : threads)
		{
			t.start();
		}
		
    Thread terminator = null;
    
    if (maxExecutionTime > 0) {
      terminator = new TerminatorThread(maxExecutionTime, threads, workload);
      terminator.start();
    }
    
    int opsDone = 0;

		for (Thread t : threads)
		{
			try
			{
				t.join();
				opsDone += ((ClientThread)t).getOpsDone();
			}
			catch (InterruptedException e)
			{
			}
		}

		long en=System.currentTimeMillis();
		
		if (terminator != null && !terminator.isInterrupted()) {
      terminator.interrupt();
    }

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
			exportMeasurements(props, opsDone, en - st);
		} catch (IOException e)
		{
			System.err.println("Could not export measurements, error: " + e.getMessage());
			e.printStackTrace();
			System.exit(-1);
		}

		System.exit(0);
	}
}
