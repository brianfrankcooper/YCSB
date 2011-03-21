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
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import com.yahoo.ycsb.Client.Operation;
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
	public enum StatusType {
		STDERR,
		STDOUT,
		STDOUT_TABDELIMITED, 
		FILE_TABDELIMITED
	}
	
	Vector<ClientThread> _threads;
	String _label;
	StatusType _statusType;
	private String _outputFile;
	private FileWriter _fw;
	
	/**
	 * The interval for reporting status.
	 */
	public static final long sleeptime=10000;

	public StatusThread(Vector<ClientThread> threads, String label, StatusType statusType, String outputFile)
	{
		_threads=threads;
		_label=label;
		_statusType=statusType;
		_outputFile = outputFile;
		_fw = null;
	}
	
	@Override
	public void interrupt() {
		if(_fw != null) {
			try {
	      _fw.flush();
	      _fw.close();
      } catch (IOException e) {
	      System.out.println("error flushing to "+_outputFile);
      }
		}
		super.interrupt();
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

		boolean canContinue = true;
		
		
		
		try {
			if(_statusType.equals(StatusType.FILE_TABDELIMITED) && _outputFile != null) {
				_fw = new FileWriter(new File(_outputFile));
			}
      if(canContinue == true) {
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
		
					double curthroughput=1000.0*(((double)(totalops-lasttotalops))/((double)(en-lasten)));
					
					lasttotalops=totalops;
					lasten=en;
					
					DecimalFormat d = new DecimalFormat("#.##");
					
					if(_statusType.equals(StatusType.STDERR)) {
						if (totalops==0) {
							System.err.println(_label+" "+(interval/1000)+" sec: "+totalops+" operations; "+Measurements.getMeasurements().getSummary());
						}
						else
						{
							System.err.println(_label+" "+(interval/1000)+" sec: "+totalops+" operations; "+d.format(curthroughput)+" current ops/sec; "+Measurements.getMeasurements().getSummary());
						}
					}
					else if (_statusType.equals(StatusType.STDOUT))
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
					else if(_statusType.equals(StatusType.STDOUT_TABDELIMITED)) {
						if (totalops==0)
						{
							if(_label != null) 
							{
								System.out.println("test\tseconds\toperations\toperations/sec\toperation type");
								System.out.println(_label+"\t"+(interval/1000)+"\t"+totalops+"\tnone yet\t"+Measurements.getMeasurements().getSummary());
							} else {
								System.out.println("seconds\toperations\toperations/sec\toperation type");
								System.out.println((interval/1000)+"\t"+totalops+"\tnone yet\t"+Measurements.getMeasurements().getSummary());
							}
						}
						else
						{
							if(_label != null) 
							{
								System.out.println(_label+"\t"+(interval/1000)+"\t"+totalops+"\t"+d.format(curthroughput)+"\t"+Measurements.getMeasurements().getSummary());
							} else {
								System.out.println((interval/1000)+"\t"+totalops+"\t"+d.format(curthroughput)+"\t"+Measurements.getMeasurements().getSummary());
							}
						}
					} else if(_statusType.equals(StatusType.FILE_TABDELIMITED)) {
						try {
		          if(_label != null) 
		          {
		          	System.out.println(_label+"\t"+(interval/1000)+"\t"+totalops+"\t"+d.format(curthroughput)+"\t"+Measurements.getMeasurements().getSummary());
		          	_fw.write(_label+"\t"+(interval/1000)+"\t"+totalops+"\t"+d.format(curthroughput)+"\t"+Measurements.getMeasurements().getSummary());
		          	
		          } else {
		          	System.out.println(_label+"\t"+(interval/1000)+"\t"+totalops+"\t"+d.format(curthroughput)+"\t"+Measurements.getMeasurements().getSummary());
		          	_fw.write((interval/1000)+"\t"+totalops+"\t"+d.format(curthroughput)+"\t"+Measurements.getMeasurements().getSummary());
		          }
	          } catch (IOException e) {
		          System.err.println(e.getMessage());
		          
		          shutAllClientsDown();
		          alldone = true;
		          
		          
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
		} catch (IOException e) {
      System.out.println(e.getMessage());
    } finally {
			if(_fw != null) {
				try {
					_fw.flush();
          _fw.close();
        } catch (IOException e) {
          System.out.println("error closing status file handle:"+e.getMessage());
        }
			}
			
		} 
		
	}

	/**
	 * force threads to stop
	 */
	private void shutAllClientsDown() {
	  for(ClientThread t: _threads) {
	  	t.stopFast();
	  }
	  
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
	static Random random=new Random();

	DB _db;
	Client.Operation _operation;
	Workload _workload;
	int _opcount;
	double _target;

	int _opsdone;
	int _threadid;
	int _threadcount;
	Object _workloadstate;
	Properties _props;
	AtomicBoolean _keepGoing;

	/**
	 * Constructor.
	 * 
	 * @param db the DB implementation to use
	 * @param operation true to do transactions, false to insert data
	 * @param workload the workload to use
	 * @param threadid the id of this thread 
	 * @param threadcount the total number of threads 
	 * @param props the properties defining the experiment
	 * @param opcount the number of operations (transactions or inserts) to do
	 * @param targetperthreadperms target number of operations per thread per ms
	 */
	public ClientThread(DB db, Operation operation, Workload workload, int threadid, int threadcount, Properties props, int opcount, double targetperthreadperms)
	{
		//TODO: consider removing threadcount and threadid
		_db=db;
		_operation =operation;
		_workload=workload;
		_opcount=opcount;
		_opsdone=0;
		_target=targetperthreadperms;
		_threadid=threadid;
		_threadcount=threadcount;
		_props=props;
		//System.out.println("Interval = "+interval);
		_keepGoing = new AtomicBoolean(true);
	}

	public void stopFast() {
	  _keepGoing.set(false);
	  
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
		      sleep(random.nextInt((int)(1.0/_target)));
		   }
		}
		catch (InterruptedException e)
		{
		   //do nothing
		}
		
		try
		{
			while ( (_opcount==0) || (_opsdone<_opcount) ) 
			{
				if(_keepGoing.get() == false) {
					break;
				}
				
				if (_operation.equals(Client.Operation.IS_TRANSACTION))
				{
					long st=System.currentTimeMillis();
	
	
	
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
									//do nothing
								}
	
							}
						}
					
				}
				else if(_operation.equals(Client.Operation.IS_INSERTION))
				{
					long st=System.currentTimeMillis();
	
					while ( (_opcount==0) || (_opsdone<_opcount) )
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
									//do nothing
								}
							}
						}
					}
				}
				else if(_operation.equals(Client.Operation.IS_TRUNCATION)) {
						if(!_workload.doTruncation(_db)) {
							throw new Exception("truncation attempt on database failed!");
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

	private static final String MEASUREMENTTYPE_PROPERTY = "measurementtype";

	public static final String OPERATION_COUNT_PROPERTY="operationcount";

	public static final String RECORD_COUNT_PROPERTY="recordcount";

	public static final String WORKLOAD_PROPERTY="workload";
	
	public static enum Operation { 
		IS_NONE,
		IS_TRANSACTION,
		IS_INSERTION,
		IS_TRUNCATION, 
	}
	
	/**
	 * Indicates how many inserts to do, if less than recordcount. Useful for partitioning
	 * the load among multiple servers, if the client is the bottleneck. Additionally, workloads
	 * should support the "insertstart" property, which tells them which record to start at.
	 */
	public static final String INSERT_COUNT_PROPERTY="insertcount";

	/**
	 * storage directory to store summary and throughput data in.
	 */
	public static final String STORAGE_DIR = "storagedir";
	public static final String DIR_KEY = "directorykey";
	
	/**
	 * summary file -- stores results of op
	 */
	
	public static final String SUMMARY_KEY = "summarykey";
	public static final String SUMMARY_FNAME = "summaryfile";


	/**
	 * thruput file -- stores results of throughput measurements
	 */
	public static final String THRUPUT_KEY = "thruputkey";
	public static final String THRUPUT_FNAME = "throughputfile";

	/**
	 * histogram file -- stores results of histogram measurements. 
	 */
	public static final String HISTOGRAM_KEY = "histogramkey";
	public static final String HISTOGRAM_FNAME = "histogram";
	
	/**
	 * 
	 */
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

	/**
	 * given a file handle that represents a directory, recursively remove all subdirs and files from it 
	 * @param sd
	 */
	private static void cleanDir(File sd) {
    for(File file : sd.listFiles()) {
    	if(file.isDirectory() == true) {
    		cleanDir(file);
    	}
    	
    	file.delete();
    }
    
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
	 * @param props
	 * @param opcount
	 * @param startTime
	 * @param endTime
	 * @param summaryFile
	 * @param latencyFile
   *
	 * @throws IOException Either failed to write to output stream or failed to close it.
	 */
	
	
	private static void exportMeasurements(Properties props, int opcount, long startTime, long endTime, String summaryFile, String latencyFile)
			throws IOException
	{
		MeasurementsExporter summaryExporter = null;
		MeasurementsExporter latencyExporter = null;
		OutputStream summaryOut = null;
		OutputStream latencyOut = null;
		try
		{
			// if no destination file is provided the results will be written to stdout
			
			
			if (summaryFile == null)
			{
				summaryOut = System.out;
			} else
			{
				summaryOut = new FileOutputStream(summaryFile);
			}

			// if no exporter is provided the default text one will be used
			String exporterStr = props.getProperty("exporter", "com.yahoo.ycsb.measurements.exporter.TextMeasurementsExporter");
			try
			{
				summaryExporter = (MeasurementsExporter) Class.forName(exporterStr).getConstructor(OutputStream.class).newInstance(summaryOut);
			} catch (Exception e)
			{
				System.err.println("Could not find exporter " + exporterStr
						+ ", will use default text reporter.");
				e.printStackTrace();
				summaryExporter = new TextMeasurementsExporter(summaryOut);
			}
			
			Date start = new Date(startTime);
			Date finish = new Date(endTime);
			
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd:hh:mm:ss");
			
			
			summaryExporter.write("START TIME","ms",sdf.format(start)+"\n");
			summaryExporter.write("END TIME","ms",sdf.format(finish)+"\n");
			summaryExporter.write("OVERALL", "RunTime(ms)", endTime - startTime);
			double throughput = 1000.0 * ((double) opcount) / ((double) (endTime - startTime));
			summaryExporter.write("OVERALL", "Throughput(ops/sec)", throughput);

			// only show measurements if any were requested.
			
			
			if(opcount != 0) {
				
				
				
				if (latencyFile == null)
				{
					latencyOut = System.out;
				} else
				{
					latencyOut = new FileOutputStream(latencyFile);
				}
				
				try
				{
					latencyExporter = (MeasurementsExporter) Class.forName(exporterStr).getConstructor(OutputStream.class).newInstance(latencyOut);
				} catch (Exception e)
				{
					System.err.println("Could not find exporter " + exporterStr
							+ ", will use default text reporter.");
					e.printStackTrace();
					latencyExporter = new TextMeasurementsExporter(latencyOut);
				}
				
				Measurements.getMeasurements().exportMeasurements(latencyExporter);
			}
		} finally
		{
			if(summaryExporter != null) {
				summaryExporter.close();
			}
			
//			if (summaryOut != null)
//			{
//				summaryOut.flush();
//				summaryOut.close();
//			}
			
			if(latencyExporter != null) {
				latencyExporter.close();
			}
			
			
		}
	}
	
	@SuppressWarnings("unchecked")
	public static void main(String[] args)
	{
		String dbname;
		Properties props=new Properties();
		Properties fileprops=new Properties();
		Client.Operation operation = Client.Operation.IS_TRANSACTION;
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
				operation = Client.Operation.IS_INSERTION;
				argindex++;
			} else if(args[argindex].compareTo("-truncate") == 0) {
				operation = Client.Operation.IS_TRUNCATION;
				argindex++;
			}
			else if (args[argindex].compareTo("-t")==0)
			{
				operation = Client.Operation.IS_TRANSACTION;
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

		int opcount = 0;
		if (operation.equals(Client.Operation.IS_TRANSACTION))
		{
			opcount=Integer.parseInt(props.getProperty(OPERATION_COUNT_PROPERTY,"0"));
		}
		else if(operation.equals(Client.Operation.IS_INSERTION))
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

		Vector<ClientThread> threads=new Vector<ClientThread>();

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

			ClientThread t=new ClientThread(db,operation,workload,threadid,threadcount,props,opcount/threadcount,targetperthreadperms);

			threads.add(t);
			//t.start();
		}
		
		// determine whether we want to output to a file structure. 
		
		String storeDir = props.getProperty(STORAGE_DIR);
		
		String summaryFile = null;
		String thruputFile = null;
		String histogramFile = null;
		
		if(storeDir  != null) {
			// does dir exist? If so, remove all files.

			File sd = new File(storeDir);
			
			if(sd.exists()) {
				cleanDir(sd);
			} else {
				sd.mkdirs();
			}
			
			summaryFile = storeDir+"/"+props.getProperty(SUMMARY_FNAME,"summary.txt");
			thruputFile = storeDir+"/"+props.getProperty(THRUPUT_FNAME,"thruput.txt");
			histogramFile = storeDir+"/"+props.getProperty(HISTOGRAM_FNAME,"histogram.txt");
			
		}

		StatusThread statusthread=null;

		StatusThread.StatusType statusType = StatusThread.StatusType.STDOUT; // default without user selection is STDOUT. 
		
		if (status)
		{
			statusType = StatusThread.StatusType.STDERR; // default to stdErr output  if user selected async status.
			
			if (props.getProperty(MEASUREMENTTYPE_PROPERTY,"").compareTo("timeseries")==0) 
			{
				statusType = StatusThread.StatusType.STDOUT;
			}	
			else if(props.getProperty(MEASUREMENTTYPE_PROPERTY,"").compareTo("tabdelimited") == 0) 
			{
				statusType = StatusThread.StatusType.STDOUT_TABDELIMITED;
			} 
			else if(props.getProperty(MEASUREMENTTYPE_PROPERTY,"").compareTo("filetabdelimited") == 0) {
				statusType = StatusThread.StatusType.FILE_TABDELIMITED;
			}
			
			statusthread=new StatusThread(threads,label,statusType,thruputFile);
			statusthread.start();
			
			statusType = StatusThread.StatusType.STDOUT; // revert back to stdout for the rest of status output. 
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
				exportMeasurements(props, opcount, st, en,summaryFile,histogramFile);
			} catch (IOException e)
			{
				System.err.println("Could not export measurements, error: " + e.getMessage());
				e.printStackTrace();
				System.exit(-1);
			}

		System.exit(0);
	}
}
