package be_uclouvain_ingi2145_p1;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import be_uclouvain_ingi2145_p1.GraphNode.Color;

/**
 * Contains Mapper and Reducer for the evaluation phase.
 * 
 * Given the internal representation of the graph along with the search state informations, 
 * it makes a file with the adjacency list for the destination node, and then use it to emits in output
 * how many iterations were done and the PoIs found.
 * It is also implemented a reducer that gives in output the set of immediate neighbors of the  
 * source node that have been used to PoIs found by the algorithm.
 * 
 * @author Filippo Projetto
 */
public class EvaluateSolution {
	
	/**
	 * Map function for the evaluation phase.
	 * 
	 * Filter the graph, emits the GRAY nodes only and build
	 * the adjacency list for the destination node.
	 *  
	 * @author Filippo Projetto
	 */
	public static class EvaluateGraphMap extends Mapper<LongWritable, GraphNode, Text, GraphNode>
	{   
		/*
		 * node id
		 */
		private String id;
		
		@Override
		protected void map(LongWritable key, GraphNode value, Context context) throws IOException, InterruptedException
		{
			final int dstId = Integer.parseInt(context.getConfiguration().get("dst")); //destination node id
			/*
			 * filename for the adjacency list of the destination
			 */
			final String dstAdjList = context.getConfiguration().get("dstAdjList");
			
			if(value.getColor() == Color.GRAY){
				id = new Integer(value.getId()).toString();
				context.write(new Text(id), value);	
			}
			
			if(value.getId() == dstId){
				/*
				 * write the adjacency list of the destination into a file
				 */
				Configuration configuration = new Configuration();
				FileSystem hdfs = FileSystem.newInstance(URI.create(dstAdjList), configuration);
				Path file = new Path(dstAdjList);

				if(hdfs.exists(file)){ 
					hdfs.delete(file, true ); 
				} 
				
				OutputStream os = hdfs.create(file);
				BufferedWriter br = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) );
				
				//get the adjacency list as a string
				String dstNeighbor = value.getEdges().toString();
				
				//skip useless characters
				dstNeighbor = dstNeighbor.replace("[", "").replace("]", "").replaceAll("\\s", "");
				
				//write it into the file
				br.write(dstNeighbor);
				br.newLine();
				
				br.close();
				hdfs.close();
			}
		}
	}

	/**
	 * Reduce function for the evaluation phase.
	 * 
	 * It gives in output the list of PoIs only.
	 * 
	 * @author Filippo Projetto
	 */
	public static class EvaluateGraphReduce extends Reducer<Text, GraphNode, Text, GraphNode>
	{
		/*
		 * node to examine in the reduce phase
		 */
		private GraphNode node;
		/*
		 * adjacency list of the destination node
		 */
		private Set<Integer> edges = new HashSet<Integer>();
		/*
		 * PoI set
		 */
		private Set<Integer> result = new HashSet<Integer>();
		/*
		 * number of iterations, to be written into the output
		 */
		private Integer iterations = -1;
		/**
		 * Read file with the adjacency list of the destination node and bring it in memory.
		 */
		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			/*
			 * filename for the adjacency list of the destination
			 */
			final String dstAdjList = context.getConfiguration().get("dstAdjList");
			
			 /* 
			  * read destination adjacency list file.
			  */
			Configuration configuration = new Configuration();
			FileSystem hdfs = FileSystem.newInstance(URI.create(dstAdjList), configuration);

			Path file = new Path(dstAdjList);
			
			if (!hdfs.exists( file )){ 
				throw new IOException("adjListDst file doesn't exist"); 
			} 
			InputStream os = hdfs.open(file);
			BufferedReader br = new BufferedReader( new InputStreamReader( os, "UTF-8" ) );
			String dstNeighbor = br.readLine();
			
			StringTokenizer stEdges = new StringTokenizer(dstNeighbor, ",");
			Integer id = new Integer(0);
			 /* 
			  * Fill the destination neighbors set with the information in the file.
			  */
			while(stEdges.hasMoreTokens()){
				try {
					id = Integer.parseInt(stEdges.nextToken());
					edges.add(id);
				}catch(NumberFormatException nfe){
					throw new IOException("Error parsing integer value in record "+dstNeighbor+", "+id.toString());
				}
			}		    			
			
			br.close();
			hdfs.close(); 
		}

		
		@Override
		protected void reduce(Text key, Iterable<GraphNode> values, Context context) throws IOException, InterruptedException
		{
			node = values.iterator().next();
			
			//get number of iteration from distance information (do it only once) 
			if(iterations == -1)
				iterations = node.getDistance();
			
			/*
			 * check if one of this node's neighbor belongs to the destination
			 * neighborhood too. If yes we found a PoI. 
			 */
			for(Integer i : node.getEdges()){
				if(edges.contains(i)){					
					//found a PoI, add it to the result set.
					result.add(i);
				}
			}
		}
		
		/**
		 * At the end of reduce phase, write the result in output. 
		 */
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			context.write(new Text(iterations.toString()+" "+result.toString().replaceAll("\\s", "")), null);
		}
	}
	
	/**
	 * Reduce function for the evaluation phase.
	 * It gives in output the set of immediate neighbors of the source 
	 * node that have been used to PoIs too.
	 * 
	 * @author Filippo Projetto
	 */
	public static class EvaluateGraphReduceExtra extends Reducer<Text, GraphNode, Text, GraphNode>
	{
		/*
		 * node to examine in the reduce phase
		 */
		private GraphNode node;
		/*
		 * adjacency list of the destination node
		 */
		private Set<Integer> edges = new HashSet<Integer>();
		/*
		 * PoI set 
		 */
		private Set<Integer> result = new HashSet<Integer>();
		/*
		 * number of iterations, to be written into the output
		 */
		private Integer iterations = -1;
		
		/*
		 * set of immediate neighbors of the source node that have been used to PoIs too.
		 */
		private Set<Integer> friends = new HashSet<Integer>();

		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			/*
			 * filename for the adjacency list of the destination
			 */
			final String dstAdjList = context.getConfiguration().get("dstAdjList");
			
			 /* 
			  * read destination adjacency list file.
			  */
			Configuration configuration = new Configuration();
			FileSystem hdfs = FileSystem.newInstance(URI.create(dstAdjList), configuration);

			Path file = new Path(dstAdjList);
			
			if (!hdfs.exists( file )){ 
				throw new IOException("adjListDst file doesn't exist"); 
			} 
			InputStream os = hdfs.open(file);
			BufferedReader br = new BufferedReader( new InputStreamReader( os, "UTF-8" ) );
			String dstNeighbor = br.readLine();
			
			StringTokenizer stEdges = new StringTokenizer(dstNeighbor, ",");
			Integer id = new Integer(0);
			 /* 
			  * Fill the destination neighbors set with the information in the file.
			  */
			while(stEdges.hasMoreTokens()){
				try {
					id = Integer.parseInt(stEdges.nextToken());
					edges.add(id);
				}catch(NumberFormatException nfe){
					throw new IOException("Error parsing integer value in record "+dstNeighbor+", "+id.toString());
				}
			}		    			
			br.close();
			hdfs.close(); 
		}

		@Override
		protected void reduce(Text key, Iterable<GraphNode> values, Context context) throws IOException, InterruptedException
		{
			node = values.iterator().next();
			
			//get number of iteration from distance information (do it only once) 
			if(iterations == -1)
				iterations = node.getDistance();
			
			/*
			 * check if one of this node's neighbor belongs to the destination
			 * neighborhood too. If yes we found a PoI. 
			 */
			for(Integer i : node.getEdges()){
				if(edges.contains(i)){			
					/*
					 * found a PoI, add it to the result set.
					 * In addition, get the set of immediate neighbors of the source node 
					 * for this PoI and add it to the friends set. 
					 */
					result.add(i);
					friends.addAll(node.getRoots());
				}
			}
		}
		
		/**
		 * At the end of reduce phase, write the result in output. 
		 * It includes the set of immediate neighbors of the source node that have been used to PoIs too.
		 */
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			context.write(new Text(iterations.toString()+" "+result.toString().replaceAll("\\s", "")), null);
			context.write(new Text(friends.toString().replaceAll("\\s", "")), null);
		}
	}
}
