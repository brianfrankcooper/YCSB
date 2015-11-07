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
 * Main class for executing YCSB.
 */
public final class Client {
  static final class SlowStartupWarningThread extends Thread {
    @Override
    public void run() {
      try {
        sleep(2000);
      } catch (final InterruptedException e) {
        return;
      }
      System.err.println(" (might take a few minutes for large data sets)");
    }
  }

  /**
   * The database class to be used.
   */
  public static final String DB_PROPERTY = "db";

  public static final String DEFAULT_RECORD_COUNT = "0";

  /**
   * If set to the path of a file, YCSB will write all output to this file
   * instead of STDOUT.
   */
  public static final String EXPORT_FILE_PROPERTY = "exportfile";

  /**
   * The exporter class to be used. The default is
   * com.yahoo.ycsb.measurements.exporter.TextMeasurementsExporter.
   */
  public static final String EXPORTER_PROPERTY = "exporter";

  /**
   * Indicates how many inserts to do, if less than recordcount. Useful for
   * partitioning the load among multiple servers, if the client is the
   * bottleneck. Additionally, workloads should support the "insertstart"
   * property, which tells them which record to start at.
   */
  public static final String INSERT_COUNT_PROPERTY = "insertcount";

  /**
   * The maximum amount of time (in seconds) for which the benchmark will be
   * run.
   */
  public static final String MAX_EXECUTION_TIME = "maxexecutiontime";

  /**
   * The target number of operations to perform.
   */
  public static final String OPERATION_COUNT_PROPERTY = "operationcount";

  /**
   * The number of records to load into the database initially.
   */
  public static final String RECORD_COUNT_PROPERTY = "recordcount";

  /**
   * Target number of operations per second.
   */
  public static final String TARGET_PROPERTY = "target";

  /**
   * The number of YCSB client threads to run.
   */
  public static final String THREAD_COUNT_PROPERTY = "threadcount";

  /**
   * The workload class to be loaded.
   */
  public static final String WORKLOAD_PROPERTY = "workload";

  public static boolean checkRequiredProperties(final Properties props) {
    if (props.getProperty(WORKLOAD_PROPERTY) == null) {
      System.out.println("Missing property: " + WORKLOAD_PROPERTY);
      return false;
    }

    return true;
  }

  public static void main(final String[] args) {
    String dbname;
    Properties props = new Properties();
    final Properties fileprops = new Properties();
    boolean dotransactions = true;
    int threadcount = 1;
    int target = 0;
    boolean status = false;
    String label = "";

    // parse arguments
    int argindex = 0;
    if (args.length == 0) {
      usageMessage();
      System.exit(0);
    }
    while (args[argindex].startsWith("-")) {
      if (args[argindex].compareTo("-threads") == 0) {
        argindex++;
        if (argindex >= args.length) {
          usageMessage();
          System.exit(0);
        }
        final int tcount = Integer.parseInt(args[argindex]);
        props.setProperty(THREAD_COUNT_PROPERTY, tcount + "");
        argindex++;
      } else if (args[argindex].compareTo("-target") == 0) {
        argindex++;
        if (argindex >= args.length) {
          usageMessage();
          System.exit(0);
        }
        final int ttarget = Integer.parseInt(args[argindex]);
        props.setProperty(TARGET_PROPERTY, ttarget + "");
        argindex++;
      } else if (args[argindex].compareTo("-load") == 0) {
        dotransactions = false;
        argindex++;
      } else if (args[argindex].compareTo("-t") == 0) {
        dotransactions = true;
        argindex++;
      } else if (args[argindex].compareTo("-s") == 0) {
        status = true;
        argindex++;
      } else if (args[argindex].compareTo("-db") == 0) {
        argindex++;
        if (argindex >= args.length) {
          usageMessage();
          System.exit(0);
        }
        props.setProperty(DB_PROPERTY, args[argindex]);
        argindex++;
      } else if (args[argindex].compareTo("-l") == 0) {
        argindex++;
        if (argindex >= args.length) {
          usageMessage();
          System.exit(0);
        }
        label = args[argindex];
        argindex++;
      } else if (args[argindex].compareTo("-P") == 0) {
        argindex = loadProps(args, fileprops, argindex);
      } else if (args[argindex].compareTo("-p") == 0) {
        argindex = addProp(args, props, argindex);
      } else {
        System.out.println("Unknown option " + args[argindex]);
        usageMessage();
        System.exit(0);
      }

      if (argindex >= args.length) {
        break;
      }
    }
    if (argindex != args.length) {
      usageMessage();
      System.exit(0);
    }

    props = mergeProperties(props, fileprops);
    if (!checkRequiredProperties(props)) {
      System.exit(0);
    }

    // get number of threads, target and db
    final long maxExecutionTime =
        Integer.parseInt(props.getProperty(MAX_EXECUTION_TIME, "0"));
    threadcount =
        Integer.parseInt(props.getProperty(THREAD_COUNT_PROPERTY, "1"));
    dbname = props.getProperty(DB_PROPERTY, "com.yahoo.ycsb.BasicDB");
    target = Integer.parseInt(props.getProperty(TARGET_PROPERTY, "0"));

    // compute the target throughput
    double targetperthreadperms = -1;
    if (target > 0) {
      final double targetperthread = ((double) target) / ((double) threadcount);
      targetperthreadperms = targetperthread / 1000.0;
    }

    printStartupHeader(args);

    // show a warning message that creating the workload is taking a while
    // but only do so if it is taking longer than 2 seconds
    // (showing the message right away if the setup wasn't taking very long was
    // confusing people)
    final Thread warningthread = new SlowStartupWarningThread();
    warningthread.start();
    // set up measurements
    Measurements.setProperties(props);
    // load the workload
    Workload workload = bootstrapWorkload(props);
    warningthread.interrupt();

    System.err.println("Starting test.");

    final List<ClientThread> clients = new ArrayList<ClientThread>(threadcount);
    final CountDownLatch completeLatch = createClients(dbname, props,
        dotransactions, threadcount, targetperthreadperms, workload, clients);

    StatusThread statusthread =
        startStatusThread(props, status, label, completeLatch, clients);

    final long start = System.currentTimeMillis();

    for (final Thread t : clients) {
      t.start();
    }

    final long endTime =
        waitForDone(status, maxExecutionTime, workload, clients, statusthread);

    int opsDone = 0;
    for (final ClientThread t : clients) {
      opsDone += t.getOpsDone();
    }
    try {
      exportMeasurements(props, opsDone, endTime - start);
    } catch (final IOException e) {
      System.err
          .println("Could not export measurements, error: " + e.getMessage());
      e.printStackTrace();
      System.exit(-1);
    }

    System.exit(0);
  }

  private static StatusThread startStatusThread(Properties props,
      boolean status, String label, final CountDownLatch completeLatch,
      final List<ClientThread> clients) {
    StatusThread statusthread = null;
    if (status) {
      boolean standardstatus = false;
      if (props.getProperty(Measurements.MEASUREMENT_TYPE_PROPERTY, "")
          .compareTo("timeseries") == 0) {
        standardstatus = true;
      }
      final int statusIntervalSeconds =
          Integer.parseInt(props.getProperty("status.interval", "10"));
      statusthread = new StatusThread(completeLatch, clients, label,
          standardstatus, statusIntervalSeconds);
      statusthread.start();
    }
    return statusthread;
  }

  private static Properties mergeProperties(Properties props,
      final Properties fileprops) {
    // set up logging
    // BasicConfigurator.configure();

    // overwrite file properties with properties from the command line

    // Issue #5 - remove call to stringPropertyNames to make compilable under
    // Java 1.5
    for (final Enumeration e = props.propertyNames(); e.hasMoreElements();) {
      final String prop = (String) e.nextElement();

      fileprops.setProperty(prop, props.getProperty(prop));
    }
    props = fileprops;
    return props;
  }

  private static Workload bootstrapWorkload(Properties props) {
    final ClassLoader classLoader = Client.class.getClassLoader();

    Workload workload = null;

    try {
      final Class workloadclass =
          classLoader.loadClass(props.getProperty(WORKLOAD_PROPERTY));

      workload = (Workload) workloadclass.newInstance();
    } catch (final Exception e) {
      e.printStackTrace();
      e.printStackTrace(System.out);
      System.exit(0);
    }

    try {
      workload.init(props);
    } catch (final WorkloadException e) {
      e.printStackTrace();
      e.printStackTrace(System.out);
      System.exit(0);
    }
    return workload;
  }

  private static void printStartupHeader(final String[] args) {
    System.out.println("YCSB Client 0.1");
    System.out.print("Command line:");
    for (final String arg : args) {
      System.out.print(" " + arg);
    }
    System.out.println();
    System.err.println("Loading workload...");
  }

  private static CountDownLatch createClients(String dbname, Properties props,
      boolean dotransactions, int threadcount, double targetperthreadperms,
      Workload workload, final List<ClientThread> clients) {

    final CountDownLatch completeLatch = new CountDownLatch(threadcount);
    int opcount;
    if (dotransactions) {
      opcount =
          Integer.parseInt(props.getProperty(OPERATION_COUNT_PROPERTY, "0"));
    } else {
      if (props.containsKey(INSERT_COUNT_PROPERTY)) {
        opcount =
            Integer.parseInt(props.getProperty(INSERT_COUNT_PROPERTY, "0"));
      } else {
        opcount = Integer.parseInt(
            props.getProperty(RECORD_COUNT_PROPERTY, DEFAULT_RECORD_COUNT));
      }
    }

    for (int threadid = 0; threadid < threadcount; threadid++) {
      DB db = null;
      try {
        db = DBFactory.newDB(dbname, props);
      } catch (final UnknownDBException e) {
        System.out.println("Unknown DB " + dbname);
        System.exit(0);
      }

      int threadopcount = opcount / threadcount;

      // ensure correct number of operations, in case opcount is not a multiple
      // of threadcount
      if (threadid < (opcount % threadcount)) {
        ++threadopcount;
      }

      final ClientThread t = new ClientThread(db, dotransactions, workload,
          props, threadopcount, targetperthreadperms, completeLatch);

      clients.add(t);
    }
    return completeLatch;
  }

  private static long waitForDone(boolean status, final long maxExecutionTime,
      Workload workload, final List<ClientThread> clients,
      StatusThread statusthread) {
    Thread terminator = null;
    if (maxExecutionTime > 0) {
      terminator = new TerminatorThread(maxExecutionTime, clients, workload);
      terminator.start();
    }

    for (final ClientThread t : clients) {
      try {
        t.join();
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    final long endTime = System.currentTimeMillis();

    if ((terminator != null) && !terminator.isInterrupted()) {
      terminator.interrupt();
    }

    if (status) {
      // wake up status thread if it's asleep
      statusthread.interrupt();
      // at this point we assume all the monitored threads are already gone as
      // per above join loop.
      try {
        statusthread.join();
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    try {
      workload.cleanup();
    } catch (final WorkloadException e) {
      e.printStackTrace();
      e.printStackTrace(System.out);
      System.exit(0);
    }
    return endTime;
  }

  private static int addProp(final String[] args, Properties props,
      int argindex) {
    argindex++;
    if (argindex >= args.length) {
      usageMessage();
      System.exit(0);
    }
    final int eq = args[argindex].indexOf('=');
    if (eq < 0) {
      usageMessage();
      System.exit(0);
    }

    final String name = args[argindex].substring(0, eq);
    final String value = args[argindex].substring(eq + 1);
    props.put(name, value);
    // System.out.println("["+name+"]=["+value+"]");
    argindex++;
    return argindex;
  }

  private static int loadProps(final String[] args, final Properties fileprops,
      int argindex) {
    argindex++;
    if (argindex >= args.length) {
      usageMessage();
      System.exit(0);
    }
    final String propfile = args[argindex];
    argindex++;

    final Properties myfileprops = new Properties();
    try {
      myfileprops.load(new FileInputStream(propfile));
    } catch (final IOException e) {
      System.out.println(e.getMessage());
      System.exit(0);
    }

    // Issue #5 - remove call to stringPropertyNames to make compilable
    // under Java 1.5
    for (final Enumeration e = myfileprops.propertyNames(); e
        .hasMoreElements();) {
      final String prop = (String) e.nextElement();

      fileprops.setProperty(prop, myfileprops.getProperty(prop));
    }
    return argindex;
  }

  public static void usageMessage() {
    System.out.println("Usage: java com.yahoo.ycsb.Client [options]");
    System.out.println("Options:");
    System.out.println(
        "  -threads n: execute using n threads (default: 1) - can also be "
            + "specified as the \n"
            + "        \"threadcount\" property using -p");
    System.out.println("  -target n: attempt to do n operations per second "
        + "(default: unlimited) - can also\n"
        + "       be specified as the \"target\" property using -p");
    System.out.println("  -load:  run the loading phase of the workload");
    System.out.println(
        "  -t:  run the transactions phase of the workload " + "(default)");
    System.out
        .println("  -db dbname: specify the name of the DB to use (default: "
            + "com.yahoo.ycsb.BasicDB) - \n"
            + "        can also be specified as the \"db\" property using -p");
    System.out.println(
        "  -P propertyfile: load properties from the given file. Multiple "
            + "files can");
    System.out
        .println("           be specified, and will be processed in the order "
            + "specified");
    System.out.println(
        "  -p name=value:  specify a property to be passed to the DB and "
            + "workloads;");
    System.out.println(
        "          multiple properties can be specified, and override any");
    System.out.println("          values in the propertyfile");
    System.out.println("  -s:  show status during run (default: no status)");
    System.out.println(
        "  -l label:  use label for status (e.g. to label one experiment "
            + "out of a whole batch)");
    System.out.println("");
    System.out.println("Required properties:");
    System.out.println("  " + WORKLOAD_PROPERTY
        + ": the name of the workload class to use (e.g. "
        + "com.yahoo.ycsb.workloads.CoreWorkload)");
    System.out.println("");
    System.out
        .println("To run the transaction phase from multiple servers, start a "
            + "separate client on each.");
    System.out.println(
        "To run the load phase from multiple servers, start a separate client "
            + "on each; additionally,");
    System.out.println(
        "use the \"insertcount\" and \"insertstart\" properties to divide up "
            + "the records to be inserted");
  }

  /**
   * Exports the measurements to either sysout or a file using the exporter
   * loaded from conf.
   * 
   * @throws IOException
   *           Either failed to write to output stream or failed to close it.
   */
  private static void exportMeasurements(final Properties props,
      final int opcount, final long runtime) throws IOException {
    MeasurementsExporter exporter = null;
    try {
      // if no destination file is provided the results will be written to
      // stdout
      OutputStream out;
      final String exportFile = props.getProperty(EXPORT_FILE_PROPERTY);
      if (exportFile == null) {
        out = System.out;
      } else {
        out = new FileOutputStream(exportFile);
      }

      // if no exporter is provided the default text one will be used
      final String exporterStr = props.getProperty(EXPORTER_PROPERTY,
          "com.yahoo.ycsb.measurements.exporter.TextMeasurementsExporter");
      try {
        exporter = (MeasurementsExporter) Class.forName(exporterStr)
            .getConstructor(OutputStream.class).newInstance(out);
      } catch (final Exception e) {
        System.err.println("Could not find exporter " + exporterStr
            + ", will use default text reporter.");
        e.printStackTrace();
        exporter = new TextMeasurementsExporter(out);
      }

      exporter.write("OVERALL", "RunTime(ms)", runtime);
      final double throughput = (1000.0 * (opcount)) / (runtime);
      exporter.write("OVERALL", "Throughput(ops/sec)", throughput);

      Measurements.getMeasurements().exportMeasurements(exporter);
    } finally {
      if (exporter != null) {
        exporter.close();
      }
    }
  }

  /**
   * Hidden Constructor.
   */
  private Client() {
    // Nothing.
  }
}

/**
 * A thread for executing transactions or data inserts to the database.
 *
 * @author cooperb
 *
 */
class ClientThread extends Thread {
  private static boolean spinSleep;

  static void sleepUntil(final long deadline) {
    long now = System.nanoTime();
    while ((now = System.nanoTime()) < deadline) {
      if (!spinSleep) {
        LockSupport.parkNanos(deadline - now);
      }
    }
  }

  private DB db;
  private boolean dotransactions;
  private final Measurements measurements;
  private int opcount;
  private int opsdone;

  private Properties props;
  private double targetOpsPerMs;
  private long targetOpsTickNs;
  private int threadcount;
  private int threadid;
  private Workload workload;
  private Object workloadstate;

  /** Counts down each of the clients completing. */
  private final CountDownLatch completeLatch;

  /**
   * Constructor.
   *
   * @param db
   *          the DB implementation to use
   * @param dotransactions
   *          true to do transactions, false to insert data
   * @param workload
   *          the workload to use
   * @param props
   *          the properties defining the experiment
   * @param opcount
   *          the number of operations (transactions or inserts) to do
   * @param targetperthreadperms
   *          target number of operations per thread per ms
   * @param completeLatch
   *          The latch tracking the completion of all clients.
   */
  ClientThread(final DB db, final boolean dotransactions,
      final Workload workload, final Properties props, final int opcount,
      final double targetperthreadperms, final CountDownLatch completeLatch) {
    this.db = db;
    this.dotransactions = dotransactions;
    this.workload = workload;
    this.opcount = opcount;
    this.opsdone = 0;
    if (targetperthreadperms > 0) {
      this.targetOpsPerMs = targetperthreadperms;
      this.targetOpsTickNs = (long) (1000000 / targetOpsPerMs);
    }
    this.props = props;
    this.measurements = Measurements.getMeasurements();
    this.spinSleep = Boolean.valueOf(props.getProperty("spin.sleep", "false"));
    this.completeLatch = completeLatch;
  }

  public int getOpsDone() {
    return opsdone;
  }

  /**
   * the total amount of work this thread is still expected to do.
   */
  public int getOpsTodo() {
    final int todo = opcount - opsdone;
    return todo < 0 ? 0 : todo;
  }

  @Override
  public void run() {
    try {
      db.init();
    } catch (final DBException e) {
      e.printStackTrace();
      e.printStackTrace(System.out);
      return;
    }

    try {
      workloadstate = workload.initThread(props, threadid, threadcount);
    } catch (final WorkloadException e) {
      e.printStackTrace();
      e.printStackTrace(System.out);
      return;
    }

    // NOTE: Switching to using nanoTime and parkNanos for time management here
    // such that the measurements
    // and the client thread have the same view on time.

    // spread the thread operations out so they don't all hit the DB at the same
    // time
    // GH issue 4 - throws exception if _target>1 because random.nextInt
    // argument must be >0
    // and the sleep() doesn't make sense for granularities < 1 ms anyway
    if ((targetOpsPerMs > 0) && (targetOpsPerMs <= 1.0)) {
      final long randomMinorDelay =
          Utils.random().nextInt((int) targetOpsTickNs);
      sleepUntil(System.nanoTime() + randomMinorDelay);
    }
    try {
      if (dotransactions) {
        final long startTimeNanos = System.nanoTime();

        while (((opcount == 0) || (opsdone < opcount))
            && !workload.isStopRequested()) {

          if (!workload.doTransaction(db, workloadstate)) {
            break;
          }

          opsdone++;

          throttleNanos(startTimeNanos);
        }
      } else {
        final long startTimeNanos = System.nanoTime();

        while (((opcount == 0) || (opsdone < opcount))
            && !workload.isStopRequested()) {

          if (!workload.doInsert(db, workloadstate)) {
            break;
          }

          opsdone++;

          throttleNanos(startTimeNanos);
        }
      }
    } catch (final Exception e) {
      e.printStackTrace();
      e.printStackTrace(System.out);
      System.exit(0);
    }

    try {
      measurements.setIntendedStartTimeNs(0);
      db.cleanup();
    } catch (final DBException e) {
      e.printStackTrace();
      e.printStackTrace(System.out);
      return;
    } finally {
      completeLatch.countDown();
    }
  }

  private void throttleNanos(final long startTimeNanos) {
    // throttle the operations
    if (targetOpsPerMs > 0) {
      // delay until next tick
      final long deadline = startTimeNanos + (opsdone * targetOpsTickNs);
      sleepUntil(deadline);
      measurements.setIntendedStartTimeNs(deadline);
    }
  }

}

/**
 * Turn seconds remaining into more useful units. i.e. if there are hours or
 * days worth of seconds, use them.
 */
final class RemainingFormatter {

  public static StringBuilder format(long seconds) {
    final StringBuilder time = new StringBuilder();
    final long days = TimeUnit.SECONDS.toDays(seconds);
    if (days > 0) {
      time.append(days).append(" days ");
      seconds -= TimeUnit.DAYS.toSeconds(days);
    }
    final long hours = TimeUnit.SECONDS.toHours(seconds);
    if (hours > 0) {
      time.append(hours).append(" hours ");
      seconds -= TimeUnit.HOURS.toSeconds(hours);
    }
    /* Only include minute granularity if we're < 1 day. */
    if (days < 1) {
      final long minutes = TimeUnit.SECONDS.toMinutes(seconds);
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

  /**
   * Hidden Constructor.
   */
  private RemainingFormatter() {
    super();
  }
}

/**
 * A thread to periodically show the status of the experiment, to reassure you
 * that progress is being made.
 *
 * @author cooperb
 */
class StatusThread extends Thread {
  /** The clients that are running. */
  private final List<ClientThread> clients;

  /** Counts down each of the clients completing. */
  private final CountDownLatch completeLatch;

  private final String label;
  
  /** The interval for reporting status. */
  private final long sleeptimeNs;

  private final boolean standardstatus;

  /**
   * Creates a new StatusThread.
   *
   * @param completeLatch
   *          The latch that each client thread will
   *          {@link CountDownLatch#countDown()} as they complete.
   * @param clients
   *          The clients to collect metrics from.
   * @param label
   *          The label for the status.
   * @param standardstatus
   *          If true the status is printed to stdout in addition to stderr.
   * @param statusIntervalSeconds
   *          The number of seconds between status updates.
   */
  StatusThread(final CountDownLatch completeLatch,
      final List<ClientThread> clients, final String label,
      final boolean standardstatus, final int statusIntervalSeconds) {
    this.completeLatch = completeLatch;
    this.clients = clients;
    this.label = label;
    this.standardstatus = standardstatus;
    this.sleeptimeNs = TimeUnit.SECONDS.toNanos(statusIntervalSeconds);
  }

  /**
   * Run and periodically report status.
   */
  @Override
  public void run() {
    final long startTimeMs = System.currentTimeMillis();
    final long startTimeNanos = System.nanoTime();
    long deadline = startTimeNanos + sleeptimeNs;
    long startIntervalMs = startTimeMs;
    long lastTotalOps = 0;

    boolean alldone;

    do {
      final long nowMs = System.currentTimeMillis();

      lastTotalOps =
          computeStats(startTimeMs, startIntervalMs, nowMs, lastTotalOps);

      alldone = waitForClientsUntil(deadline);

      startIntervalMs = nowMs;
      deadline += sleeptimeNs;
    } while (!alldone);

    // Print the final stats.
    computeStats(startTimeMs, startIntervalMs, System.currentTimeMillis(),
        lastTotalOps);
  }

  /**
   * Computes and prints the stats.
   *
   * @param startTimeMs
   *          The start time of the test.
   * @param startIntervalMs
   *          The start time of this interval.
   * @param endIntervalMs
   *          The end time (now) for the interval.
   * @param lastTotalOps
   *          The last total operations count.
   *
   * @return The current operation count.
   */
  private long computeStats(final long startTimeMs, final long startIntervalMs,
      final long endIntervalMs, final long lastTotalOps) {
    final SimpleDateFormat format =
        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");

    long totalops = 0;
    long todoops = 0;

    // Calculate the total number of operations completed.
    for (final ClientThread t : clients) {
      totalops += t.getOpsDone();
      todoops += t.getOpsTodo();
    }

    final long interval = endIntervalMs - startTimeMs;
    final double throughput =
        1000.0 * (((double) totalops) / (double) interval);
    final double curthroughput = 1000.0 * (((double) (totalops - lastTotalOps))
        / ((double) (endIntervalMs - startIntervalMs)));
    final long estremaining = (long) Math.ceil(todoops / throughput);

    final DecimalFormat d = new DecimalFormat("#.##");
    final String fullLabel = label + format.format(new Date());

    final StringBuilder msg = new StringBuilder(fullLabel).append(" ")
        .append(interval / 1000).append(" sec: ");
    msg.append(totalops).append(" operations; ");

    if (totalops != 0) {
      msg.append(d.format(curthroughput)).append(" current ops/sec; ");
    }
    if (todoops != 0) {
      msg.append("est completion in ")
          .append(RemainingFormatter.format(estremaining));
    }

    msg.append(Measurements.getMeasurements().getSummary());

    System.err.println(msg);

    if (standardstatus) {
      System.out.println(msg);
    }
    return totalops;
  }

  /**
   * Waits for all of the client to finish or the deadline to expire.
   *
   * @param deadline
   *          The current deadline.
   *
   * @return True if all of the clients completed.
   */
  private boolean waitForClientsUntil(final long deadline) {
    boolean alldone = false;
    long now = System.nanoTime();

    while (!alldone && (now < deadline)) {
      try {
        alldone = completeLatch.await(deadline - now, TimeUnit.NANOSECONDS);
      } catch (final InterruptedException ie) {
        // If we are interrupted the thread is being asked to shutdown.
        // Return true to indicate that and reset the interrupt state
        // of the thread.
        Thread.currentThread().interrupt();
        alldone = true;
      }
      now = System.nanoTime();
    }

    return alldone;
  }
}
