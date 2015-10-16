package be_uclouvain_ingi2145_p1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Appender;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;
import org.apache.log4j.varia.LevelRangeFilter;

import be_uclouvain_ingi2145_p1.GraphNode.Color;

import com.sun.corba.se.spi.ior.Writeable;

public class PoIDriver extends Configured implements Tool
{
    /**
     * Singleton instance.
     */
    public static PoIDriver GET;

    /**
     * Author's name.
     */
    public static final String NAME = "Filippo Projetto";

    // ---------------------------------------------------------------------------------------------

    public static void main(String[] args) throws Exception
    {
        // configure log4j to output to a file
        Logger logger = LogManager.getRootLogger();
        logger.addAppender(new FileAppender(new SimpleLayout(), "p1.log"));

        // configure log4j to output to the console
        Appender consoleAppender = new ConsoleAppender(new SimpleLayout());
        LevelRangeFilter filter = new LevelRangeFilter();
        // switch to another level for more detail (own (INGI2145) messages use FATAL)
        filter.setLevelMin(Level.ERROR);
        consoleAppender.addFilter(filter);
        // (un)comment to (un)mute console output
        logger.addAppender(consoleAppender);

        // switch to Level.DEBUG or Level.TRACE for more detail
        logger.setLevel(Level.INFO);

        GET = new PoIDriver();
        Configuration conf = new Configuration();

        int res = ToolRunner.run(conf, GET, args);
        System.exit(res);
    }

    // ---------------------------------------------------------------------------------------------

    // Depending on your implementation the number of stages might differ
    // Consider this skeleton as a outline you should be able to change it according to your needs. 
    @Override
    public int run(String[] args) throws Exception
    {
        System.out.println(NAME);

        if (args.length == 0) {
            args = new String[]{ "command missing" };
        }

        switch (args[0]) {
            case "init":
                init(args[1], args[2], args[3], args[4], Integer.parseInt(args[5]));
                break;
            case "iter":
                iter(args[1], args[2], args[3], args[4], Integer.parseInt(args[5]), Integer.parseInt(args[6]));
                break;
            case "evaluate":
                evaluate(args[1], args[2], args[3], args[4], Integer.parseInt(args[5]));
                break;
            case "composite":
                composite(args[1], args[2], args[3], args[4], Integer.parseInt(args[5]), Integer.parseInt(args[6]));
                break;
            default:
                System.out.println("Unknown command: " + args[0]);
                break;
        }

        return 0;
    }

    // ---------------------------------------------------------------------------------------------

    void init(String inputDir, String outputDir, String srcId, String dstId, int nReducers) throws Exception
    {
        Logger.getRootLogger().fatal("[INGI2145] init");

        // TODO
        
        Job job = Utils.configureJob(inputDir, outputDir, GetGraphMap.class, null, GetGraphReduce.class, LongWritable.class, LongWritable.class, LongWritable.class, GraphNode.class, nReducers);
        job.setOutputFormatClass(GraphOutputFormat.class);
        job.getConfiguration().set("src", srcId);
        Utils.startJob(job);
        
    }

    // ---------------------------------------------------------------------------------------------
    //iterNo is the value of iteration counter

    void iter(String inputDir, String outputDir, String srcId, String dstId, int iterNo, int nReducers) throws Exception
    {
        Logger.getRootLogger().fatal("[INGI2145] iter: " + inputDir + " (to) " + outputDir);
        
        // TODO
        Job job = Utils.configureJob(inputDir, outputDir, GetGraphMap.class, null, GetGraphReduce.class, LongWritable.class, LongWritable.class, LongWritable.class, GraphNode.class, nReducers);
        job.setOutputFormatClass(GraphOutputFormat.class);
        job.getConfiguration().set("src", srcId);
        Utils.startJob(job);
    }


    // ---------------------------------------------------------------------------------------------

    void evaluate(String inputDir, String outputDir, String srcId, String dstId, int nReducers) throws Exception
    {
        Logger.getRootLogger().fatal("[INGI2145] evaluate from:" + inputDir);

        // TODO
    }

    // ---------------------------------------------------------------------------------------------
    // maxHops is the maximum number of hops in which the destination should be reached
    void composite(String inputDir, String outputDir, String srcId, String dstId, int maxHops, int nReducers) throws Exception
    {
        Logger.getRootLogger().fatal("[INGI2145] composite: " + inputDir + " (to) " + outputDir);
        
        boolean found;
        int nIter;

        // TODO: run init task
        String initInputDir = "";
        String initOutputDir = "";
        int initNReducers = 0; 
         
        init(initInputDir, initOutputDir, srcId, dstId, initNReducers);
        
        // TODO: iterative phase
        found = false;
        nIter = 0;
        while(!found && nIter < maxHops){
         // TODO: iter + evaluate 
        }

        // TODO: place the output in the right place
        
    }
    
    public static class GetGraphMap extends Mapper<LongWritable, Text, LongWritable, LongWritable>
    {   
        private LongWritable node1;
        private LongWritable node2;
        private String[] word;
        
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException
        {
        	word = value.toString().split(" ");
        	node1 = new LongWritable(Integer.parseInt(word[0]));
        	node2 = new LongWritable(Integer.parseInt(word[1]));
        	
        	context.write(node1, node2);
        	context.write(node2, node1);
        }
    }
    
    public static class GetGraphReduce extends Reducer<LongWritable, LongWritable, LongWritable, GraphNode>
    {
    	
        @Override
        protected void reduce(LongWritable key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException
        {
        	String srcId = context.getConfiguration().get("src");
        	String k = key.toString();
        	
        	StringBuffer sb = new StringBuffer();
        	
        	
        	sb.append(k+" ");
        	// TODO produce the right string according to output format
        	Iterator<LongWritable> it = values.iterator();
        	sb.append(it.next()+",");
        	
        	while(it.hasNext()){
        		sb.append(","+it.next());
        	}
        	GraphNode g = new GraphNode(sb.toString());
        	
        	if(k.compareTo(srcId) == 0){
        		g.distance = 0;
        		g.color = Color.GRAY;
        	}
        	
        	context.write(key, g);
        }
    }
    
    public static class GraphSearchMap extends Mapper<LongWritable, GraphNode, LongWritable, GraphNode>
    {   
        @Override
        protected void map(LongWritable key, GraphNode value, Context context)
            throws IOException, InterruptedException
        {
        	context.write(key, value);
        }
    }
    
    public static class GraphSearchReduce extends Reducer<IntWritable, IntWritable, IntWritable, Text>
    {
        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException
        {
        	StringBuffer sb = new StringBuffer();
        	
        	for(IntWritable value : values){
        		sb.append(value+" ");
        	}
        	
        	context.write(key, new Text(sb.toString()));
        }
    }
    
    }

