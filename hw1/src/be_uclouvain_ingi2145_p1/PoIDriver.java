package be_uclouvain_ingi2145_p1;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.commons.math3.linear.Array2DRowFieldMatrix;
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
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

import com.sun.corba.se.impl.orbutil.graph.Graph;

import be_uclouvain_ingi2145_p1.GraphNode.Color;

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
        
        Job job = Utils.configureJob(inputDir, outputDir, GetGraphMap.class, null, GetGraphReduce.class, Text.class, Text.class, Text.class, GraphNode.class, nReducers);
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
        Job job = Utils.configureJob(inputDir, outputDir, GraphSearchMap.class, null, GraphSearchReduce.class, Text.class, GraphNode.class, Text.class, GraphNode.class, nReducers);
        
        job.setOutputFormatClass(GraphOutputFormat.class);
        job.setInputFormatClass(GraphInputFormat.class);
        
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
    
    public static class GetGraphMap extends Mapper<LongWritable, Text, Text, Text>
    {   
        private Text node1 = new Text();
        private Text node2 = new Text();
        private String[] word;
        
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException
        {
        	word = value.toString().split(" ");
        	node1.set(word[0]);
        	node2.set(word[1]);
        	
        	context.write(node1, node2);
        	context.write(node2, node1);
        }
    }
    
    public static class GetGraphReduce extends Reducer<Text, Text, Text, GraphNode>
    {
    	
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException
        {
        	String srcId = context.getConfiguration().get("src");
        	
        	
        	StringBuffer sb = new StringBuffer();
        	
        	
        	sb.append(key+" ");
        	// TODO produce the right string according to output format
        	Iterator<Text> it = values.iterator();
        	sb.append(it.next()+",");
        	
        	while(it.hasNext()){
        		sb.append(","+it.next());
        	}
        	GraphNode g = new GraphNode(sb.toString());
        	
        	if(key.toString().compareTo(srcId) == 0){
        		g.distance = 0;
        		g.color = Color.GRAY;
        	}
        	
        	context.write(key, g);
        }
    }
    
    public static class GraphSearchMap extends Mapper<LongWritable, GraphNode, Text, GraphNode>
    {   
    	Text keyNeighbor = new Text();
		private Text K = new Text();
    
        @Override
        protected void map(LongWritable key, GraphNode value, Context context)
            throws IOException, InterruptedException
        {
        	
        	//Logger.getRootLogger().fatal("[INGI2145] mapper input edges: "+value.edges);
        	if (value.color == Color.GRAY) {
    			for (Integer neighbor : value.edges) { // for all the adjacent nodes of
    												// the gray node

    				GraphNode adjacentNode = new GraphNode(neighbor); // create a new node

    				//adjacentNode.id = neighbor; // set the id of the node
    				adjacentNode.distance = value.distance +1; // set the distance of the node, the distance of the adjacentNode is set to be the distance
    				//of its predecessor node+ 1, this is done since we consider a graph of unit edge weights
    				adjacentNode.color = Color.GRAY; // set the color of the node to be GRAY
    				adjacentNode.parentId = value.id; // set the parent of the node, if the adjacentNode is already visited by some other parent node, it is not update in the reducer
    				//this is because the nodes are processed in terms of levels from the source node, i.e all the nodes in the level 1 are processed first, then the nodes in the level 2 and so on.
    				adjacentNode.nEdges = 0;
    				//emit the information about the adjacent node in the form of key-value pair where the key is the node Id and the value is the associated information
    				//for the nodes emitted here, the list of adjacent nodes will be empty in the value part, the reducer will merge this with the original list of adjacent nodes associated with a node
    				
    				keyNeighbor.set(neighbor.toString());
    				context.write(keyNeighbor, adjacentNode);
    				Logger.getRootLogger().fatal("[INGI2145] map emit: "+keyNeighbor + " "+adjacentNode.color + " "+adjacentNode.distance);
    				//Logger.getRootLogger().fatal("[INGI2145] mapper input neigh edges: "+adjacentNode.edges);

    			}
    			// this node is done, color it black
    			value.color = Color.BLACK;
    		}

    		// No matter what, emit the input node
    		// If the node came into this method GRAY, it will be output as BLACK
    		//otherwise, the nodes that are already colored BLACK or WHITE are emitted by setting the key as the node Id and the value as the node's information
    	
        	
        	K.set(""+value.id);
    		context.write(K, value);
    		value.color = Color.WHITE;
    		context.write(K, value);
    		Logger.getRootLogger().fatal("[INGI2145] map emit: "+value.id+" "+value.edges);
    		//Logger.getRootLogger().fatal("[INGI2145] mapper input edges: "+value.edges);
        }
    }
    
    public static class GraphSearchReduce extends Reducer<Text, GraphNode, Text, GraphNode>
    {
    	
    	//GraphNode outnode, curr;
    	@Override
        protected void reduce(Text key, Iterable<GraphNode> values, Context context)
            throws IOException, InterruptedException
        {
    		GraphNode outnode = new GraphNode(Integer.parseInt(key.toString()));
    		
    		
        	for(GraphNode curr : values){
        		if (curr.distance < outnode.distance) {
    				outnode.distance = curr.distance;
    				outnode.parentId = curr.parentId;
    			}
        		
        		if(outnode.nEdges == 0 && curr.nEdges != 0){
        			Logger.getRootLogger().fatal("[INGI2145] assigning edges to "+ key + ": "+curr.nEdges);
        			outnode.edges = new ArrayList<Integer>(curr.edges);
        			outnode.nEdges = curr.nEdges;
        		}
        		Logger.getRootLogger().fatal("[INGI2145] curr.color: "+ curr.color + ", outnode.color: "+outnode.color);
        		if(curr.color.ordinal() > outnode.color.ordinal()){
        			outnode.color = curr.color;
        		}
        	}
        	/*
    		//since the values are of the type Iterable, iterate through the values associated with the key
    		//for all the values corresponding to a particular node id
        	Iterator<GraphNode> it = values.iterator();
        	
        	outnode =  it.next();
        	Logger.getRootLogger().fatal("[INGI2145] outnode.distance: "+outnode.distance);
        	it.next();
        	Logger.getRootLogger().fatal("[INGI2145] outnode.distance: "+outnode.distance);
        	
        	Logger.getRootLogger().fatal("[INGI2145] nedges for "+key+": "+outnode.nEdges);
        	
        	while(it.hasNext()){
        		curr = it.next();
        		Logger.getRootLogger().fatal("[INGI2145] curr.distance: "+ curr.distance + ", outnode.distance: "+outnode.distance);
        		if (curr.distance < outnode.distance) {
    				outnode.distance = curr.distance;
    				outnode.parentId = curr.parentId;
    			}
        		
        		if(outnode.nEdges == 0 && curr.nEdges != 0){
        			Logger.getRootLogger().fatal("[INGI2145] assigning edges to "+ key + ": "+curr.nEdges);
        			outnode.edges = new ArrayList<Integer>(curr.edges);
        			outnode.nEdges = curr.nEdges;
        		}
        		Logger.getRootLogger().fatal("[INGI2145] curr.color: "+ curr.color + ", outnode.color: "+outnode.color);
        		if(curr.color.ordinal() > outnode.color.ordinal()){
        			outnode.color = curr.color;
        		}
        	}
        	*/
        	Logger.getRootLogger().fatal("[INGI2145] node: "+key+", edges: "+outnode.edges + ", color: "+outnode.color);
    		//emit the key, value pair where the key is the node id and the value is the node's information
    		context.write(key, outnode);		
        }
    }
    
    }

