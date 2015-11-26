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


import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import com.yahoo.ycsb.measurements.Measurements;
import com.yahoo.ycsb.measurements.exporter.MeasurementsExporter;
import com.yahoo.ycsb.measurements.exporter.TextMeasurementsExporter;

/**
 * A thread to periodically show the status of the experiment, to reassure you that progress is being made.
 *
 * @author cooperb
 */
class StatusThread extends Thread
{
  /** Counts down each of the clients completing. */
  private final CountDownLatch _completeLatch;

  /** The clients that are running. */
  private final List<ClientThread> _clients;

  private final String _label;
  private final boolean _standardstatus;

  /** The interval for reporting status. */
  private long _sleeptimeNs;

  /**
   * Creates a new StatusThread.
   *
   * @param completeLatch The latch that each client thread will {@link CountDownLatch#countDown()} as they complete.
   * @param clients The clients to collect metrics from.
   * @param label The label for the status.
   * @param standardstatus If true the status is printed to stdout in addition to stderr.
   * @param statusIntervalSeconds The number of seconds between status updates.
   */
  public StatusThread(CountDownLatch completeLatch, List<ClientThread> clients,
      String label, boolean standardstatus, int statusIntervalSeconds)
  {
    _completeLatch=completeLatch;
    _clients=clients;
    _label=label;
    _standardstatus=standardstatus;
    _sleeptimeNs=TimeUnit.SECONDS.toNanos(statusIntervalSeconds);
  }

  /**
   * Run and periodically report status.
   */
  @Override
  public void run()
  {
    final long startTimeMs=System.currentTimeMillis();
    final long startTimeNanos = System.nanoTime();
    long deadline = startTimeNanos + _sleeptimeNs;
    long startIntervalMs=startTimeMs;
    long lastTotalOps=0;

    boolean alldone;

    do
    {
      long nowMs=System.currentTimeMillis();

      lastTotalOps = computeStats(startTimeMs, startIntervalMs, nowMs, lastTotalOps);

      alldone = waitForClientsUntil(deadline);

      startIntervalMs=nowMs;
      deadline+=_sleeptimeNs;
    }
    while (!alldone);

    // Print the final stats.
    computeStats(startTimeMs, startIntervalMs, System.currentTimeMillis(), lastTotalOps);
  }

  /**
   * Computes and prints the stats.
   *
   * @param startTimeMs The start time of the test.
   * @param startIntervalMs The start time of this interval.
   * @param endIntervalMs The end time (now) for the interval.
   * @param lastTotalOps The last total operations count.
   *
   * @return The current operation count.
   */
  private long computeStats(final long startTimeMs, long startIntervalMs, long endIntervalMs,
      long lastTotalOps) {
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");

    long totalops=0;
    long todoops=0;

    // Calculate the total number of operations completed.
    for (ClientThread t : _clients)
    {
      totalops+=t.getOpsDone();
      todoops+=t.getOpsTodo();
    }


    long interval=endIntervalMs-startTimeMs;
    double throughput=1000.0*(((double)totalops)/(double)interval);
    double curthroughput=1000.0*(((double)(totalops-lastTotalOps))/((double)(endIntervalMs-startIntervalMs)));
    long estremaining = (long) Math.ceil(todoops / throughput);


    DecimalFormat d = new DecimalFormat("#.##");
    String label = _label + format.format(new Date());

    StringBuilder msg = new StringBuilder(label).append(" ").append(interval/1000).append(" sec: ");
    msg.append(totalops).append(" operations; ");

    if (totalops != 0) {
      msg.append(d.format(curthroughput)).append(" current ops/sec; ");
    }
    if (todoops != 0) {
        msg.append("est completion in ").append(RemainingFormatter.format(estremaining));
    }

    msg.append(Measurements.getMeasurements().getSummary());

    System.err.println(msg);

    if (_standardstatus) {
      System.out.println(msg);
    }
    return totalops;
  }

  /**
   * Waits for all of the client to finish or the deadline to expire.
   *
   * @param deadline The current deadline.
   *
   * @return True if all of the clients completed.
   */
  private boolean waitForClientsUntil(long deadline) {
    boolean alldone=false;
    long now=System.nanoTime();

    while( !alldone && now < deadline ) {
      try {
        alldone = _completeLatch.await(deadline-now, TimeUnit.NANOSECONDS);
      }
      catch( InterruptedException ie) {
        // If we are interrupted the thread is being asked to shutdown.
        // Return true to indicate that and reset the interrupt state
        // of the thread.
        Thread.currentThread().interrupt();
        alldone=true;
      }
      now=System.nanoTime();
    }

    return alldone;
  }
}

/**
 * Turn seconds remaining into more useful units.
 * i.e. if there are hours or days worth of seconds, use them.
 */
class RemainingFormatter {
	public static StringBuilder format(long seconds) {
		StringBuilder time = new StringBuilder();
		long days = TimeUnit.SECONDS.toDays(seconds);
		if (days > 0) {
			time.append(days).append(" days ");
			seconds -= TimeUnit.DAYS.toSeconds(days);
		}
		long hours = TimeUnit.SECONDS.toHours(seconds);
		if (hours > 0) {
			time.append(hours).append(" hours ");
			seconds -= TimeUnit.HOURS.toSeconds(hours);
		}
		/* Only include minute granularity if we're < 1 day. */
		if (days < 1) {
			long minutes = TimeUnit.SECONDS.toMinutes(seconds);
			if (minutes > 0) {
				time.append(minutes).append(" minutes ");
				seconds -= TimeUnit.MINUTES.toSeconds(seconds);
			}
		}
		/* Only bother to include seconds if we're < 1 minute */
		if (time.length() == 0) {
			time.append(seconds).append(" seconds ");
		}
		return time;
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
  /** Counts down each of the clients completing. */
  private final CountDownLatch _completeLatch;

  private static boolean _spinSleep;
  DB _db;
  boolean _dotransactions;
  Workload _workload;
  int _opcount;
  double _targetOpsPerMs;

  int _opsdone;
  int _threadid;
  int _threadcount;
  Object _workloadstate;
  Properties _props;
  long _targetOpsTickNs;
  final Measurements _measurements;

  /**
   * Constructor.
   *
   * @param db the DB implementation to use
   * @param dotransactions true to do transactions, false to insert data
   * @param workload the workload to use
   * @param props the properties defining the experiment
   * @param opcount the number of operations (transactions or inserts) to do
   * @param targetperthreadperms target number of operations per thread per ms
   * @param completeLatch The latch tracking the completion of all clients.
   */
  public ClientThread(DB db, boolean dotransactions, Workload workload, Properties props, int opcount, double targetperthreadperms, CountDownLatch completeLatch)
  {
    _db=db;
    _dotransactions=dotransactions;
    _workload=workload;
    _opcount=opcount;
    _opsdone=0;
    if(targetperthreadperms > 0){
      _targetOpsPerMs=targetperthreadperms;
      _targetOpsTickNs=(long)(1000000/_targetOpsPerMs);
    }
    _props=props;
    _measurements = Measurements.getMeasurements();
    _spinSleep = Boolean.valueOf(_props.getProperty("spin.sleep", "false"));
    _completeLatch=completeLatch;
  }

  public int getOpsDone()
  {
    return _opsdone;
  }

  @Override
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

    //NOTE: Switching to using nanoTime and parkNanos for time management here such that the measurements
    // and the client thread have the same view on time.

    //spread the thread operations out so they don't all hit the DB at the same time
    // GH issue 4 - throws exception if _target>1 because random.nextInt argument must be >0
    // and the sleep() doesn't make sense for granularities < 1 ms anyway
    if ((_targetOpsPerMs > 0) && (_targetOpsPerMs <= 1.0))
    {
      long randomMinorDelay = Utils.random().nextInt((int) _targetOpsTickNs);
      sleepUntil(System.nanoTime() + randomMinorDelay);
    }
    try
    {
      if (_dotransactions)
      {
        long startTimeNanos = System.nanoTime();

        while (((_opcount == 0) || (_opsdone < _opcount)) && !_workload.isStopRequested())
        {

          if (!_workload.doTransaction(_db,_workloadstate))
          {
            break;
          }

          _opsdone++;

          throttleNanos(startTimeNanos);
        }
      }
      else
      {
        long startTimeNanos = System.nanoTime();

        while (((_opcount == 0) || (_opsdone < _opcount)) && !_workload.isStopRequested())
        {

          if (!_workload.doInsert(_db,_workloadstate))
          {
            break;
          }

          _opsdone++;

          throttleNanos(startTimeNanos);
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
      _measurements.setIntendedStartTimeNs(0);
      _db.cleanup();
    }
    catch (DBException e)
    {
      e.printStackTrace();
      e.printStackTrace(System.out);
      return;
    }
    finally
    {
      _completeLatch.countDown();
    }
  }

  static void sleepUntil(long deadline) {
    long now = System.nanoTime();
    while((now = System.nanoTime()) < deadline) {
      if (!_spinSleep) {
        LockSupport.parkNanos(deadline - now);
      }
    }
  }
  private void throttleNanos(long startTimeNanos) {
    //throttle the operations
    if (_targetOpsPerMs > 0)
    {
      // delay until next tick
      long deadline = startTimeNanos + _opsdone*_targetOpsTickNs;
      sleepUntil(deadline);
      _measurements.setIntendedStartTimeNs(deadline);
    }
  }
  
  /**
   * the total amount of work this thread is still expected to do
   */
  public int getOpsTodo()
  {
    int todo = _opcount - _opsdone;
    return todo < 0 ? 0 : todo;
  }
}

/**
 * Main class for executing YCSB.
 */
public class Client
{

  public static final String DEFAULT_RECORD_COUNT = "0";

  /**
   * The target number of operations to perform.
   */
  public static final String OPERATION_COUNT_PROPERTY="operationcount";

  /**
   * The number of records to load into the database initially.
   */
  public static final String RECORD_COUNT_PROPERTY="recordcount";

  /**
   * The workload class to be loaded.
   */
  public static final String WORKLOAD_PROPERTY="workload";

  /**
   * The database class to be used.
   */
  public static final String DB_PROPERTY="db";

  /**
   * The exporter class to be used. The default is
   * com.yahoo.ycsb.measurements.exporter.TextMeasurementsExporter.
   */
  public static final String EXPORTER_PROPERTY="exporter";

  /**
   * If set to the path of a file, YCSB will write all output to this file
   * instead of STDOUT.
   */
  public static final String EXPORT_FILE_PROPERTY="exportfile";

  /**
   * The number of YCSB client threads to run.
   */
  public static final String THREAD_COUNT_PROPERTY="threadcount";

  /**
   * Indicates how many inserts to do, if less than recordcount. Useful for partitioning
   * the load among multiple servers, if the client is the bottleneck. Additionally, workloads
   * should support the "insertstart" property, which tells them which record to start at.
   */
  public static final String INSERT_COUNT_PROPERTY="insertcount";

  /**
   * Target number of operations per second
   */
  public static final String TARGET_PROPERTY="target";

  /**
   * The maximum amount of time (in seconds) for which the benchmark will be run.
   */
  public static final String MAX_EXECUTION_TIME = "maxexecutiontime";


  public static void usageMessage()
  {
    System.out.println("Usage: java com.yahoo.ycsb.Client [options]");
    System.out.println("Options:");
    System.out.println("  -threads n: execute using n threads (default: 1) - can also be specified as the \n" +
        "        \"threadcount\" property using -p");
    System.out.println("  -target n: attempt to do n operations per second (default: unlimited) - can also\n" +
        "       be specified as the \"target\" property using -p");
    System.out.println("  -load:  run the loading phase of the workload");
    System.out.println("  -t:  run the transactions phase of the workload (default)");
    System.out.println("  -db dbname: specify the name of the DB to use (default: com.yahoo.ycsb.BasicDB) - \n" +
        "        can also be specified as the \"db\" property using -p");
    System.out.println("  -P propertyfile: load properties from the given file. Multiple files can");
    System.out.println("           be specified, and will be processed in the order specified");
    System.out.println("  -p name=value:  specify a property to be passed to the DB and workloads;");
    System.out.println("          multiple properties can be specified, and override any");
    System.out.println("          values in the propertyfile");
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
      String exportFile = props.getProperty(EXPORT_FILE_PROPERTY);
      if (exportFile == null)
      {
        out = System.out;
      } else
      {
        out = new FileOutputStream(exportFile);
      }

      // if no exporter is provided the default text one will be used
      String exporterStr = props.getProperty(EXPORTER_PROPERTY, "com.yahoo.ycsb.measurements.exporter.TextMeasurementsExporter");
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
      double throughput = 1000.0 * (opcount) / (runtime);
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
        props.setProperty(THREAD_COUNT_PROPERTY, tcount+"");
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
        props.setProperty(TARGET_PROPERTY, ttarget+"");
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
        props.setProperty(DB_PROPERTY,args[argindex]);
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
    threadcount=Integer.parseInt(props.getProperty(THREAD_COUNT_PROPERTY,"1"));
    dbname=props.getProperty(DB_PROPERTY,"com.yahoo.ycsb.BasicDB");
    target=Integer.parseInt(props.getProperty(TARGET_PROPERTY,"0"));

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
      @Override
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
        opcount=Integer.parseInt(props.getProperty(RECORD_COUNT_PROPERTY, DEFAULT_RECORD_COUNT));
      }
    }

    CountDownLatch completeLatch=new CountDownLatch(threadcount);
    final List<ClientThread> clients=new ArrayList<ClientThread>(threadcount);
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


      int threadopcount = opcount/threadcount;

      // ensure correct number of operations, in case opcount is not a multiple of threadcount
      if (threadid<opcount%threadcount)
      {
        ++threadopcount;
      }

      ClientThread t=new ClientThread(db,dotransactions,workload,props,threadopcount, targetperthreadperms, completeLatch);

      clients.add(t);
    }

    StatusThread statusthread=null;

    if (status)
    {
      boolean standardstatus=false;
      if (props.getProperty(Measurements.MEASUREMENT_TYPE_PROPERTY,"").compareTo("timeseries")==0)
      {
        standardstatus=true;
      }
      int statusIntervalSeconds = Integer.parseInt(props.getProperty("status.interval","10"));
      statusthread=new StatusThread(completeLatch,clients,label,standardstatus,statusIntervalSeconds);
      statusthread.start();
    }

    long st=System.currentTimeMillis();

    for (Thread t : clients)
    {
      t.start();
    }

    Thread terminator = null;

    if (maxExecutionTime > 0) {
      terminator = new TerminatorThread(maxExecutionTime, clients, workload);
      terminator.start();
    }

    int opsDone = 0;

    for (Thread t : clients)
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
      // wake up status thread if it's asleep
      statusthread.interrupt();
      // at this point we assume all the monitored threads are already gone as per above join loop.
      try {
        statusthread.join();
      } catch (InterruptedException e) {
      }
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
