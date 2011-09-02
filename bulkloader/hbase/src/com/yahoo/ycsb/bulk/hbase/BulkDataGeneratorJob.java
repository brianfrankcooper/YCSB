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

package com.yahoo.ycsb.bulk.hbase;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.StringTokenizer;
import java.util.TimeZone;


import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapred.JobConf;
//import org.apache.hadoop.mapred.FileOutputFormat;
//import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
// import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
//import org.apache.hadoop.mapred.lib.NLineInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/* These are specific to HBase, they need to be replaced with
 * generic implementations instead
 */
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.KeyValue;

import com.yahoo.ycsb.bulk.hbase.RangePartitioner;

/* TODO need to remove this dependency */


/*
 * - synthetically generate input file based on parameters
 *  
 */
public class BulkDataGeneratorJob extends Configured implements Tool {

    /** Names of the the configuration parameters */
    public static final String ARG_KEY_JOBNAME     = "tsw.datagen.jobname";
    public static final String ARG_KEY_OUTDIR      = "tsw.datagen.outdir";
    
    /** key generation parameters */
    public static final String ARG_KEY_ROW_PREFIX  = "tsw.datagen.rowprefix";
    public static final String ARG_KEY_RANGE_START = "tsw.datagen.rangestart";
    public static final String ARG_KEY_RANGE_END   = "tsw.datagen.rangeend";
    public static final String ARG_KEY_HASH_KEYS   = "tsw.datagen.hashkeys";
    public static final String ARG_KEY_HASHED_RANGE_START
                                    = "tsw.datagen.hashkeys.rangestart";
    public static final String ARG_KEY_HASHED_RANGE_END
                                    = "tsw.datagen.hashkeys.rangeend";

    /** number of partitions (ranges) for key generation. */
    public static final String ARG_KEY_RANGE_PARTITIONS
                                    = "tsw.datagen.rangepartitions";
    public static final String ARG_KEY_GEN_RANGE_PARTITIONS
                                    = "tsw.datagen.generaterangepartitions";
    public static final String ARG_KEY_SPLITFILE   = "tsw.datagen.splitfile";

    /** Whether or not to query the system to get the table splits. */
    public static final String ARG_KEY_GETTABLESPLITS = "tsw.datagen.gettablesplits";
    /** Info to obtain the splits for the table */
    public static final String ARG_KEY_TABLE       = "tsw.datagen.table";
    public static final String ARG_KEY_USER        = "tsw.datagen.user";
    public static final String ARG_KEY_PASSWORD    = "tsw.datagen.pwd";

    /** Whether or not the table splits should be generated based on the generated keys */
    public static final String ARG_KEY_GENTABLESPLITS = "tsw.datagen.generatetablesplits";
    /** Number of splits to generate.  Keep in mind that the number of generated 
     * ranges will be splitscount + 1.
     */
    public static final String ARG_KEY_SPLIT_COUNT   = "tsw.datagen.splitcount";

    public static final String ARG_KEY_GEN_RANDOM_VALUES = "tsw.datagen.genrandomvalues";
    public static final String ARG_KEY_COLUMN_COUNT  = "tsw.datagen.columncount";
    /** Name of the column family */
    public static final String ARG_KEY_COLUMN_NAME   = "tsw.datagen.columnname";
    /** Prefix for the column qualifier */      
    public static final String ARG_KEY_COLUMN_PREFIX = "tsw.datagen.columnprefix";
    public static final String ARG_KEY_DATASIZE      = "tsw.datagen.datasize";
    public static final String ARG_KEY_RANDOM_SEED   = "tsw.datagen.randomseed";
    public static final String ARG_KEY_TIMESTAMP     = "tsw.datagen.timestamp";
    public static final String ARG_KEY_SORTKEYS      = "tsw.datagen.sortkeys";
    public static final String ARG_KEY_NUMREDUCE     = "tsw.datagen.numreduce"; 
    
    public static final String[] ARG_KEYS = {
        ARG_KEY_ROW_PREFIX,
        ARG_KEY_RANGE_START,
        ARG_KEY_RANGE_END,
        ARG_KEY_USER,
        ARG_KEY_TABLE,
        ARG_KEY_PASSWORD,
        ARG_KEY_RANGE_PARTITIONS,
        ARG_KEY_SPLITFILE,
        ARG_KEY_GETTABLESPLITS,
        ARG_KEY_HASH_KEYS,
        ARG_KEY_GEN_RANDOM_VALUES,
        ARG_KEY_COLUMN_COUNT,
        ARG_KEY_COLUMN_PREFIX,
        ARG_KEY_DATASIZE,
        ARG_KEY_GEN_RANGE_PARTITIONS,
        ARG_KEY_RANDOM_SEED,
        ARG_KEY_TIMESTAMP,
        ARG_KEY_SORTKEYS
    };

    public static void printKeyValues( Configuration conf, String[] keys,
                                       PrintStream out )  {
        for (String k: keys) {
            out.print( k );
            out.print( '=' );
            out.println( conf.get( k ) );
        }

	out.println();
    }


    public BulkDataGeneratorJob() {}


    public BulkDataGeneratorJob(Configuration conf) {
        super( conf );
    }
    
    public static abstract class RowGeneratorMapperBase<OutKey,OutVal>
            extends Mapper<LongWritable, Text, OutKey, OutVal> {
	
        protected Text    outputKey   = new Text();
        protected Text    outputValue = new Text();
        protected long    start;
        protected long    end;
        protected String  rowPrefix;
        protected boolean hashKey;
        protected long    cellCount;
        protected int     hashStart;
        protected int     hashEnd;

        protected void setup( Context context )
                throws IOException, InterruptedException {
            super.setup( context );
            this.cellCount = 0;
            Configuration conf = context.getConfiguration();
            
            this.rowPrefix = conf.get( ARG_KEY_ROW_PREFIX, "row" );
            this.hashKey   = conf.getBoolean( ARG_KEY_HASH_KEYS, false );
            hashStart = conf.getInt( ARG_KEY_RANGE_START, 0 );
            hashEnd   = conf.getInt( ARG_KEY_RANGE_END, 0 );

            /* TODO Replace this with log statements */
            printKeyValues( conf, ARG_KEYS, System.err );
            System.err.println( "Row prefix: " + this.rowPrefix );
            System.err.println( "hashKey: " + this.hashKey );
        }

        protected void cleanup( Context context )
            throws IOException, InterruptedException {
            System.err.print( "Cell count: " + this.cellCount );
        }

        void getRange( Text value ) {
            StringTokenizer tokenizer = new StringTokenizer( value.toString() );
            String startStr = tokenizer.nextToken();
            String endStr = tokenizer.nextToken();

            this.start = Long.parseLong( startStr );
            this.end   = Long.parseLong( endStr );
        }

        protected abstract void emitOutput( String k, DataGenerator gen,
                                            long rowid, Context output )
            throws IOException, InterruptedException; 

        /**
         * @param value The input is expected to be a line with the following
         * format: start_range end_range
         */
        @Override
        public void map( LongWritable key, Text value, Context output )
                throws IOException, InterruptedException {
            String k;
            int count = 0;
            this.getRange( value );     // parse line
            long rowid = this.start;
            DataGenerator gen = new DataGenerator( this.rowPrefix, this.start,
                                                   this.end, this.hashKey,
                                                   this.hashEnd - this.hashStart + 1,
                                                   this.hashStart );
            // generate all the keys in here
            while ((k = gen.getNextKey()) != null) {
                emitOutput( k, gen, rowid, output );
                rowid++;
                count++;
            }

            System.err.println( "Processed range: " + this.start + " -- "
                                + this.end + ", count: " + count );
        }
    }

    /**
     * Mapper used for jobs that require a reduce phase.  This method only
     * emits the row key (no column family name, qualifier, ts, nor value).
     */
    public static class RowGeneratorMapper
            extends RowGeneratorMapperBase<Text, Text> {

        protected void emitOutput( String k, DataGenerator gen, long rowid,
                                   Context output )
                throws IOException, InterruptedException {
            this.cellCount++;
            this.outputKey.set( k );
            output.write( this.outputKey, this.outputValue );
        }
    }

    /**
     * Class to generate the cell values.  For each cell, it can generate
     * a predetermined
     * pattern depending on the column qualifier, or it can
     * generate (printable) random values.
     */
    public static class RowValuesGenerator extends ValueGenerator {

        int    columnCount;
        long   ts;
        String columnName;
        Text   tColumnName;
        String qualifierPrefix;
        Text[] qualifiers;
        ImmutableBytesWritable iKey;

        public static RowValuesGenerator createGenerator( Configuration conf ) {
            boolean genRandomValues
                = conf.getBoolean( ARG_KEY_GEN_RANDOM_VALUES, true );
            long seed      = conf.getLong( ARG_KEY_RANDOM_SEED, 0 );
            int datasize   = conf.getInt( ARG_KEY_DATASIZE, 10 );
            int colCount   = conf.getInt( ARG_KEY_COLUMN_COUNT, 1 );
            String colName = conf.get( ARG_KEY_COLUMN_NAME, "column" );
            String qPrefix = conf.get( ARG_KEY_COLUMN_PREFIX, "" );
            long ts = conf.getLong( ARG_KEY_TIMESTAMP,
                                    System.currentTimeMillis() );

            System.err.println( "seed: " + seed);
            System.err.println( "datasize: " + datasize);
            System.err.println( "colCount: " + colCount);
            System.err.println( "qPrefix: "  + qPrefix);
            System.err.println( "timestamp:" + ts );

            RowValuesGenerator gen
                = new RowValuesGenerator( datasize, genRandomValues, seed,
                                          colCount, colName, qPrefix, ts );

            return gen;
        }


        public RowValuesGenerator( int datasize, boolean genRandomValues,
                long seed, int colCount, String colName,
                String qPrefix, long ts ) {
            super( datasize, genRandomValues, seed );
            this.init( datasize, genRandomValues, seed, colCount, colName,
                       qPrefix, ts );        
        }

        public void init( int datasize, boolean genRandomValues,  long seed,
			  int colCount, String colName, String qPrefix,
			  long ts ) {

            this.columnCount     = colCount;
            this.columnName      = colName;
            this.tColumnName     = new Text( colName );
            this.qualifierPrefix = qPrefix;
            this.qualifiers      = new Text[colCount];
            this.ts              = ts;
	    this.iKey            = new ImmutableBytesWritable();

            StringBuilder sb = new StringBuilder( this.qualifierPrefix );
            int prefixLen = sb.length();

            /* generate column qualifier names */
            for (int i = 0; i < colCount; i++) {
                sb.append(i);
                this.qualifiers[i] = new Text( sb.toString() );
                sb.delete( prefixLen, sb.length() );
            }

            Arrays.sort( qualifiers );
        }

        public <KI,VI> int
        emitValues( String k, DataGenerator gen, long rowid,
                    TaskInputOutputContext<KI, VI, ImmutableBytesWritable,
                    KeyValue> output )
                throws IOException, InterruptedException {

            int i;
            byte[] kBytes = k.getBytes();
            byte[] cBytes = this.tColumnName.getBytes();

            // generate the keys for each of the columns
            for (i = 0; i < this.columnCount; i++) {
                byte[] qualBytes = this.qualifiers[i].getBytes();
                byte[] v = this.getContentForCell( rowid, i );

                KeyValue kv = new KeyValue( kBytes, cBytes, qualBytes, ts,
                                            KeyValue.Type.Put, v );
                this.iKey.set( kv.getBuffer(), kv.getKeyOffset(),
                               kv.getKeyLength() );   
                output.write( iKey, kv );
            }

            return i;
        }
    }
    
    
    public static class RowGeneratorMapOnly
        extends RowGeneratorMapperBase<ImmutableBytesWritable, KeyValue> {

        RowValuesGenerator valueGen;

        protected void setup( Context context )
                throws IOException, InterruptedException {
            super.setup( context );
            Configuration conf = context.getConfiguration();
            this.valueGen = RowValuesGenerator.createGenerator( conf );
        }
        
        protected void emitOutput( String k, DataGenerator gen, long rowid,
                                   Context output )
                throws IOException, InterruptedException{
            /*
             * Only when the rows need to be sorted and the row keys are
             * hashed, the rows need to be sorted (through a shuffle and
             * reduce phase).
             * If a reduce is needed, then only the key is emitted.
             * In the case of map only case, then all the row values (i.e.,
             * cells for all the column qualifiers) need to be emitted
             */
            this.cellCount += valueGen.emitValues( k, gen, rowid, output );
        }
    }

    public static class RowGeneratorReduce
            extends Reducer<Text, Text, ImmutableBytesWritable, KeyValue> {

        RowValuesGenerator valueGen;
        long               cellCount = 0;
        
        protected void setup( Context context )
                throws IOException, InterruptedException {
            super.setup( context );
            Configuration conf = context.getConfiguration();
            this.valueGen = RowValuesGenerator.createGenerator( conf );
            this.cellCount = 0;
        }


        public void reduce( Text key, Iterable<Text> values, Context output )
            throws IOException, InterruptedException  {
            /* this reducer ignores the values, it simply uses the key to
             * generate the entries to be inserted into CB
             */
            this.cellCount += valueGen.emitValues( key.toString(), null,
                                                   this.cellCount, output );
        }
    }

    /**
     * This function computes the split points for a range 
     * [start, end].
     * @param start starting point for the range to split.
     * @param end   ending point for the range to split.
     * @param numsplits number of splits to create for the range.
     * @return a list of split points in text format.
     */
    public static void writeRanges(long start, long end,
            int partitionCount, PrintStream out) {
        long rangeSize = (end - start + 1) / partitionCount;
        long rangeStart = start;
        long rangeEnd   = start - 1 + rangeSize;

        while (rangeStart <= end) {
            if (rangeEnd > end) {
                rangeEnd = end;
            }
            out.print( rangeStart );
            out.print( ' ' );
            out.println( rangeEnd );
            rangeStart += rangeSize;
	    rangeEnd    = rangeStart + rangeSize - 1;
        }
    }


    /** Create the input file used for launching the maps */
    void createInputFile( Job job, String workdir ) throws IOException {
        Configuration conf = job.getConfiguration();
        FileSystem fs      = FileSystem.get(conf);
        Path inpath        = new Path( workdir + "/inputkeyranges.txt" );
        PrintStream out = new PrintStream(
                            new BufferedOutputStream(fs.create(inpath)));
        long start = conf.getLong( ARG_KEY_RANGE_START, 0 );
        long end   = conf.getLong( ARG_KEY_RANGE_END, 0 );
        int  parts = conf.getInt( ARG_KEY_RANGE_PARTITIONS, 1 );

        writeRanges( start, end, parts, out );
        out.close();

        TextInputFormat.setInputPaths(job, inpath);
        // NLineInputFormat.setInputPaths(job, inpath);

        /* compute the max input split size */
//        long max_split = fs.getFileStatus( inpath ).getLen() / parts;
//        TextInputFormat.setMaxInputSplitSize(job, max_split);

	// JobConf jc = new JobConf(conf);
	// jc.setNumMapTasks(parts);
    }

    int createSplitsFile( Configuration conf, String splitsFile )
            throws IOException, InvalidInputException {
        int splitCount = conf.getInt( ARG_KEY_SPLIT_COUNT, 0 );

        if (splitCount <= 0) {
            throw new InvalidInputException("Invalid or unspecified split count:"
                    + splitCount + "\nSpecify it in: " + ARG_KEY_SPLIT_COUNT);
        }

        String rowPrefix = conf.get( ARG_KEY_ROW_PREFIX, "row" );
        String rowFormat = DataGenerator.getKeyFormat( rowPrefix );
        boolean hashKeys = conf.getBoolean( ARG_KEY_HASH_KEYS, false );
        long start = conf.getInt( ARG_KEY_RANGE_START, 0 );
        long end   = conf.getInt( ARG_KEY_RANGE_END, 0 );

        FileSystem fs   = FileSystem.get( conf );
        Path splitsPath = new Path( splitsFile );
        Path plainPath  = new Path( splitsFile + "-debug" );
        PrintStream out = new PrintStream(
                            new BufferedOutputStream(fs.create(splitsPath)));
        PrintStream plain = new PrintStream(
			       new BufferedOutputStream(fs.create(plainPath)));

        if (hashKeys) {
            start = conf.getInt( ARG_KEY_HASHED_RANGE_START, 0 );
            end   = conf.getInt( ARG_KEY_HASHED_RANGE_END, Integer.MAX_VALUE );
        }

        long rangeSize = Math.max( 1, (end - start + 1) / (splitCount + 1) );
        long rangeStart = start + rangeSize;

        System.err.println( "Generating splits file: " + splitsFile + 
                "\nrangeStart:" + rangeStart + "\nrangeSize: " + rangeSize +
                "\nsplitCount: " + splitCount + "\nrangeEnd: " + end );

        int i = 0;
        try {
            while (rangeStart < end && splitCount > 0) {
                out.println( new String(Base64.encodeBase64(
                            String.format(rowFormat, rangeStart).getBytes())));
		plain.println( String.format( rowFormat, rangeStart ) );
                rangeStart += rangeSize;
                splitCount--;
                i++;
            }
        } finally {
            out.close();
	    plain.close();
        }
        System.err.println( "Splits created: " + i );
        return i;
    }

    /**
     * The splits file should be generated according to these parameters:
     * - OK: flags specifying whether or not the splits file should be
     *   generated
     * - OK: #of splits / partitions for the inserts, this should be
     *   explicitly specified.
     * - OK: the name of the generated file (to store the split info).
     */
    public String getSplitsFile( Job job, String workdir )
            throws IOException, InvalidInputException {

        Configuration conf = job.getConfiguration();

        /* Read property for splits file */
        String splitsFile = conf.get( ARG_KEY_SPLITFILE,
                                      workdir + "/autogen-splits.txt" );
        boolean genSplits = conf.getBoolean( ARG_KEY_GENTABLESPLITS, false );

        if (genSplits) {
            // generate splits
            this.createSplitsFile( conf, splitsFile );
        }

        return splitsFile;
    }

    /**
     * Parameters for bulk loader specified through the config file:
     *
     * - prefix for the row keys
     * - range start
     * - range end (inclusive)
     * - num splits (or number of partitions).
     * - user
     * - password
     * - table
     *
     * For the accepted default options
     * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
     */
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();

        Util.printArgs( "run", args, System.err );
        printKeyValues( conf, ARG_KEYS, System.err );

        if (args.length > 1 || 
            (args.length == 1 && "-help".compareToIgnoreCase( args[0] ) == 0)) {
            System.err.println( "Usage: " + this.getClass().getName()
                + "input_path [generic options] [input_paths...] ouptut_path");
            GenericOptionsParser.printGenericCommandUsage( System.err );
            return 1;
        }

        // Time run
        long startTime = System.currentTimeMillis();
        String workdir;

        if (args.length == 1) {
            /* override workdir in the config if it is specified in the
             * command line
             */
            conf.set( ARG_KEY_OUTDIR, args[0] );
            workdir = args[0];
        }

        workdir = conf.get( ARG_KEY_OUTDIR );

	if (workdir == null) {
	    System.err.println( "No output directory specified" );
	    return 1;
	}

        /* Initialize job, check parameters and decide which mapper to use */
        Job job = new Job(conf, conf.get(ARG_KEY_JOBNAME,
                                         "YCSB KV data generator"));

        /* these settings are the same (i.e., fixed) independent of the
         * parameters */
        job.setJarByClass(this.getClass());
        // job.setInputFormatClass(TextInputFormat.class);
        job.setInputFormatClass( NLineInputFormat.class );

        /* these settings should depend on the type of output file */
        job.setOutputFormatClass( HFileOutputFormat.class );
        /* not sure the next two are needed */
        job.setOutputKeyClass( ImmutableBytesWritable.class );
        job.setOutputValueClass( KeyValue.class );

        this.createInputFile( job, workdir );

        HFileOutputFormat.setOutputPath( job, new Path( workdir + "/files" ) );

        /* depending on whether the keys need to be sorted and hashed, then
         * decide which mapper and reducer to use 
         */
        boolean hashKeys = conf.getBoolean( ARG_KEY_HASH_KEYS, false );
        boolean sortKeys = conf.getBoolean( ARG_KEY_SORTKEYS, true );

        /* get splits file name: side-effect -> this may generate a splits file  */
        String splitsfile = this.getSplitsFile( job,  workdir ); 
       
        if (sortKeys && hashKeys) {     /* do a full map reduce job */
            job.setMapperClass( RowGeneratorMapper.class );
            job.setMapOutputKeyClass( Text.class );
            job.setMapOutputValueClass( Text.class );
            job.setPartitionerClass( RangePartitioner.class );


            if (splitsfile == null) {
                /* Auto generate the splits file either from:
                 * - the input key ranges
                 * - from the current table splits
                 */
                throw new InvalidInputException("No splits specified");
            }

            /* Set splits file */
            RangePartitioner.setSplitFile(job, splitsfile);

            /* Add reducer (based on mapper code) */
            job.setReducerClass(RowGeneratorReduce.class);

            /* the number of reducers is dependent on the number of
             * partitions
             */
            int numReduce =conf.getInt( ARG_KEY_NUMREDUCE, 1 );
            job.setNumReduceTasks( numReduce );
        } else {                        /* perform a map only job */
            job.setMapperClass(RowGeneratorMapOnly.class);
            /* map output key and value types are the same as
             * for the job
             */
            job.setMapOutputKeyClass( job.getOutputKeyClass() );
            job.setMapOutputValueClass( job.getOutputValueClass() );
            job.setNumReduceTasks( 0 );
        }

        job.waitForCompletion( true );

//        JobClient.runJob(conf);
        SimpleDateFormat df = new SimpleDateFormat("yyyy.MM.dd HH:mm:ss.SSS z");
        SimpleDateFormat ddf = new SimpleDateFormat("HH:mm:ss.SSS");
        ddf.setTimeZone( TimeZone.getTimeZone( "UTC" ) );
        long endTime = System.currentTimeMillis();
        System.out.println( "Start time (ms): " + df.format(new Date(startTime))
                            + " -- " + startTime );
        System.out.println( "End time (ms): " + df.format(new Date(endTime)) +
                            " -- " + endTime );
        System.out.println( "Elapsed time (ms): " + ddf.format(endTime - startTime)
                            + " -- " + (endTime - startTime) );
        return 0;
    }

    public static void main(String[] args) throws Exception {
        Util.printArgs( "main", args, System.err );
        Tool tool = new BulkDataGeneratorJob( new Configuration() );
        int res   = ToolRunner.run( tool, args );
        System.exit( res );
    }
}
