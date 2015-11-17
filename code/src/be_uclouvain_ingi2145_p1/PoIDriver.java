package be_uclouvain_ingi2145_p1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
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

import be_uclouvain_ingi2145_p1.EvaluateSolution.*;
import be_uclouvain_ingi2145_p1.GetGraph.*;
import be_uclouvain_ingi2145_p1.GraphSearch.*;

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

	/**
	 * Folder name where will be stored all the temporary 
	 * files and directories during the computation.
	 */
	private static String baseDir;
	
	/**
	 * File name where to store the adjacency list for the destination node.
	 * It is used during the evaluation phase.
	 */
	private static final String dstAdjList = "/dstAdjList";
	
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
		filter.setLevelMin(Level.ALL);
		consoleAppender.addFilter(filter);
		// (un)comment to (un)mute console output
		logger.addAppender(consoleAppender);

		// switch to Level.DEBUG or Level.TRACE for more detail
		logger.setLevel(Level.INFO);

		GET = new PoIDriver();
		Configuration conf = new Configuration();

		if(args.length < 6){
			Logger.getRootLogger().fatal("[INGI2145] invalid command line. It should have 6 arguments at least.");
			return;
		}
		
		// pattern: /src-<src id>-dst-<dst id>
		baseDir = "/src-"+args[3]+"-dst-"+args[4];		
		
		int res = ToolRunner.run(conf, GET, args);
		System.exit(res);
	}

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
		case "compositeExtra":
			compositeExtra(args[1], args[2], args[3], args[4], Integer.parseInt(args[5]), Integer.parseInt(args[6]));
			break;
		default:
			System.out.println("Unknown command: " + args[0]);
			break;
		}

		return 0;
	}

	// ---------------------------------------------------------------------------------------------

	/**
	 * This job reads the file(s) in the input directory, convert them into the intermediate  format,  
	 * and output the data to the output directory, using  the  specified  number  of  reducers.
	 *   
	 * @param inputDir directory including the text representation of the social graph 
	 * @param outputDir this directory will contain the internal representation for the social graph
	 * @param srcId source node id 
	 * @param dstId destination node id
	 * @param nReducers 
	 * @throws Exception 
	 * 
	 */
	void init(String inputDir, String outputDir, String srcId, String dstId, int nReducers) throws Exception
	{
		Logger.getRootLogger().fatal("[INGI2145] init");
		
		/*
		 * Note: it's used a different class for combiners
		 */
		Job job = Utils.configureJob(inputDir, outputDir, GetGraphMap.class, GetGraphCombiner.class, GetGraphReduce.class, Text.class, Text.class, Text.class, GraphNode.class, nReducers);
		/*
		 * custom output format class: see  GraphOutputFormat file
		 */
		job.setOutputFormatClass(GraphOutputFormat.class); 
		
		/*
		 * Make available source and destination nodes' id to mappers and reducers
		 */
		job.getConfiguration().set("src", srcId); //source node's id
		job.getConfiguration().set("dst", dstId); //destination node's id
		
		Utils.startJob(job);
	}

	/**
	 * This job performs a single round of Person of Interest algorithm by reading data in  
	 * intermediate format from the input directory, and writing data in intermediate format
	 * to the output directory, using the specified number of reducers.
	 * 
	 * @param inputDir directory including the internal representation for the social graph
	 * @param outputDir this directory will contain the internal representation for the social graph
	 * updated with the iteration work 
	 * @param srcId source node id 
	 * @param dstId destination node id
	 * @param iterNo specifies which round of the algorithm is taking place, starting to count from 1
	 * @param nReducers
	 * @throws Exception
	 * 
	 */
	void iter(String inputDir, String outputDir, String srcId, String dstId, int iterNo, int nReducers) throws Exception
	{
		Logger.getRootLogger().fatal("[INGI2145] iter: " + inputDir + " (to) " + outputDir);

		/*
		 * It is used the same class as Reducer and Combiner as well: the operation is both associative and commutative
		 */
		Job job = Utils.configureJob(inputDir, outputDir, GraphSearchMap.class, GraphSearchReduce.class, GraphSearchReduce.class, Text.class, GraphNode.class, Text.class, GraphNode.class, nReducers);
		
		/*
		 * custom input and output format classes: see GraphInputFormat and GraphOutputFormat files 
		 */
		job.setOutputFormatClass(GraphOutputFormat.class);
		job.setInputFormatClass(GraphInputFormat.class);

		/*
		 * Make available source node's id to mappers and reducers
		 */
		job.getConfiguration().set("src", srcId);
		
		Utils.startJob(job);
	}


	/**
	 * This job reads data in intermediate format from the output directory of the iterative job
	 * and evaluates whether the algorithm should terminate or not. In addition, it writes the output
	 * of the evaluation in a intermediate output directory.
	 * 
	 * @param inputDir directory including the internal representation for the social graph
	 * @param outputDir
	 * @param srcId source node id
	 * @param dstId destination node id
	 * @param nReducers
	 * @throws Exception
	 * 
	 */
	void evaluate(String inputDir, String outputDir, String srcId, String dstId, int nReducers) throws Exception
	{
		Logger.getRootLogger().fatal("[INGI2145] evaluate from:" + inputDir);
		
		Job job = Utils.configureJob(inputDir, outputDir, EvaluateGraphMap.class, null, EvaluateGraphReduce.class, Text.class, GraphNode.class, Text.class, NullWritable.class, nReducers);

		/*
		 * custom input format class: see  GraphOutputFormat file
		 */
		job.setInputFormatClass(GraphInputFormat.class);
		
		/*
		 * Make available to mappers and reducers the destination node's id 
		 * and the relative path of the file where to store the adjacency list
		 * for the destination node. 
		 */
		job.getConfiguration().set("dst", dstId);
		job.getConfiguration().set("dstAdjList", outputDir+dstAdjList);
		
		Utils.startJob(job);
	}

	/**
	 * This function runs the entire Person of Interest algorithm from beginning to end, that
	 * means until at least a PoI has found or the maximum number of hops nHops has exceeded.
	 * Finally, the result file is moved to the output directory.
	 *  
	 * @param inputDir
	 * @param outputDir
	 * @param srcId source node id
	 * @param dstId destination node id
	 * @param maxHops maximum number of hops in which the destination should be reached
	 * @param nReducers
	 * @throws Exception
	 * 
	 */
	void composite(String inputDir, String outputDir, String srcId, String dstId, int maxHops, int nReducers) throws Exception
	{
		Logger.getRootLogger().fatal("[INGI2145] composite: " + inputDir + " (to) " + outputDir);
		
		//flag, a PoI it's found if set as true
		boolean found;
		int nIter;
		
		//directories for intermediate steps
		String inDir, outDir, outputDirEvaluation;
		
		/*
		 * set input and output directories for the init phase
		 */
		inDir = inputDir;
		outDir = baseDir+"/tmp1";
		init(inDir, outDir, srcId, dstId, nReducers);
		
		outputDirEvaluation = baseDir+"/result1";
		evaluate(outDir, outputDirEvaluation, srcId, dstId, nReducers);
		/*
		 * check if a PoI it's already found. 
		 * No iteration is needed if the node is only 2 hops away.
		 */
		found = Utils.checkResults(outputDirEvaluation);
		
		nIter = 0;
		
		/*
		 * until at least a PoI has found or the maximum number of hops nHops has exceeded
		 */
		while(!found && nIter < maxHops-2){
			/*
			 * build appropriate names for input and output directories of this iteration
			 */
			inDir = baseDir+"/tmp"+(nIter+1);
			outDir = baseDir+"/tmp"+(nIter+2);
			iter(inDir, outDir, srcId, dstId, nIter, nReducers);
			
			/*
			 * the output directory of iter phase becomes the input for the evaluation.
			 * It will be also the input for the next iter phase, if needed.
			 */
			outputDirEvaluation = baseDir+"/result"+(nIter+2);
			evaluate(outDir, outputDirEvaluation, srcId, dstId, nReducers);
			
			/*
			 * check if a PoI has already found.
			 */
			found = Utils.checkResults(outputDirEvaluation);
			
			nIter++;
		}
		
		/*
		 * move the result to the output directory specified by the command arguments
		 */
		Utils.moveResults(outputDirEvaluation, outputDir);
	}
	
	/**
	 * This job reads data in intermediate format from the output directory of the iterative job
	 * and evaluates whether the algorithm should terminate or not. In addition, it writes the output
	 * of the evaluation in a intermediate output directory.
	 * 
	 * It is different from the <evaluate> function because here the reducer gives in output
	 * the immediate neighbors of the source node that have been used to reach the PoIs. 
	 * 
	 * @param inputDir directory including the internal representation for the social graph
	 * @param outputDir
	 * @param srcId source node id
	 * @param dstId destination node id
	 * @param nReducers
	 * @throws Exception
	 * 
	 */
	void evaluateExtra(String inputDir, String outputDir, String srcId, String dstId, int nReducers) throws Exception
	{
		Logger.getRootLogger().fatal("[INGI2145] evaluate from:" + inputDir);
		
		Job job = Utils.configureJob(inputDir, outputDir, EvaluateGraphMap.class, null, EvaluateGraphReduceExtra.class, Text.class, GraphNode.class, Text.class, NullWritable.class, nReducers);

		/*
		 * custom input format class: see  GraphOutputFormat file
		 */
		job.setInputFormatClass(GraphInputFormat.class);
		
		/*
		 * Make available to mappers and reducers the destination node's id 
		 * and the relative path of the file where to store the adjacency list
		 * for the destination node. 
		 */
		job.getConfiguration().set("dst", dstId);
		job.getConfiguration().set("dstAdjList", outputDir+dstAdjList);
		
		Utils.startJob(job);
	}
	
	/**
	 * This function runs the entire Person of Interest algorithm from beginning to end, that
	 * means until at least a PoI has found or the maximum number of hops nHops has exceeded.
	 * Finally, the result file is moved to the output directory.
	 * 
	 * It differs from the <composite> function only because internally calls <evaluateExtra>
	 * function instead of <evaluate> to perform the solution evaluation task.
	 * 
	 * @param inputDir
	 * @param outputDir
	 * @param srcId source node id
	 * @param dstId destination node id
	 * @param maxHops maximum number of hops in which the destination should be reached
	 * @param nReducers
	 * @throws Exception
	 * 
	 */
	void compositeExtra(String inputDir, String outputDir, String srcId, String dstId, int maxHops, int nReducers) throws Exception
	{
		Logger.getRootLogger().fatal("[INGI2145] composite: " + inputDir + " (to) " + outputDir);
		
		//flag, a PoI it's found if set as true
		boolean found;
		int nIter;
		
		//directories for intermediate steps
		String inDir, outDir, outputDirEvaluation;
		
		/*
		 * set input and output directories for the init phase
		 */
		inDir = inputDir;
		outDir = baseDir+"/tmp1";
		init(inDir, outDir, srcId, dstId, nReducers);
		
		outputDirEvaluation = baseDir+"/result1";
		evaluate(outDir, outputDirEvaluation, srcId, dstId, nReducers);
		/*
		 * check if a PoI it's already found. 
		 * No iteration is needed if the node is only 2 hops away.
		 */
		found = Utils.checkResults(outputDirEvaluation);
		
		nIter = 0;
		
		/*
		 * until at least a PoI has found or the maximum number of hops nHops has exceeded
		 */
		while(!found && nIter < maxHops-2){
			/*
			 * build appropriate names for input and output directories of this iteration
			 */
			inDir = baseDir+"/tmp"+(nIter+1);
			outDir = baseDir+"/tmp"+(nIter+2);
			iter(inDir, outDir, srcId, dstId, nIter, nReducers);
			
			/*
			 * the output directory of iter phase becomes the input for the evaluation.
			 * It will be also the input for the next iter phase, if needed.
			 */
			outputDirEvaluation = baseDir+"/result"+(nIter+2);
			evaluateExtra(outDir, outputDirEvaluation, srcId, dstId, nReducers);
			
			/*
			 * check if a PoI has already found.
			 */
			found = Utils.checkResults(outputDirEvaluation);
			
			nIter++;
		}
		
		/*
		 * move the result to the output directory specified by the command arguments
		 */
		Utils.moveResults(outputDirEvaluation, outputDir);
	}
}

