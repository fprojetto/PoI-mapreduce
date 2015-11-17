package be_uclouvain_ingi2145_p1;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import be_uclouvain_ingi2145_p1.GraphNode.Color;

/**
 * Contains Mapper and Reducer for the initialization phase.
 * 
 * Given a text file as input in the right format (<nodeid> <space> <nodeid>)
 * representing connections, it builds the internal representation of the graph
 * used to the others steps of the computation. 
 * 
 * @author Filippo Projetto
 */
public class GetGraph {
	
	/**
	 * Map function for the initialization phase.
	 * 
	 * Given a text file representing connections among nodes, it produces 
	 * the output to be passed to the reducer to build the graph.
	 * 
	 * Each line in the text file represents a mutual connection, for each one it 
	 * gives in output two records, one for each side of the connection.
	 * 
	 * @author Filippo Projetto
	 */
	public static class GetGraphMap extends Mapper<LongWritable, Text, Text, Text>
	{   
		/*
		 * node id
		 */
		private Text node1 = new Text();
		/*
		 * node id
		 */
		private Text node2 = new Text();
		/*
		 * used to split each input line in two nodes 
		 */
		private String[] word;

		/**
		 * @param key line number in the text file
		 * @param value line content
		 * @param context
		 */
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			/*
			 * Get the two nodes' id from a text line. 
			 * The two id can be separated by whatever space character.
			 */
			word = value.toString().split("\\s");
		
			node1.set(word[0]);
			node2.set(word[1]);
			
			/*
			 * Two records to represent the undirected edge. 
			 */
			context.write(node1, node2);
			context.write(node2, node1);
		}
	}

	/**
	 * Combiner function for the initialization phase.
	 * 
	 * Builds a partial adjacency list and gives it in output in a text format. 
	 * 
	 * @author Filippo Projetto
	 */
	public static class GetGraphCombiner extends Reducer<Text, Text, Text, Text>
	{
		/*
		 * emitted value: text representation for a node
		 */
		private Text value;
		
		/**
		 * @param key graph node's id
		 * @param values adjacent nodes' id
		 * @param context
		 */
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			StringBuffer sb = new StringBuffer(); //stores the partial adjacency list
			
			Iterator<Text> it = values.iterator();
			
			if(it.hasNext()){
				sb.append(it.next());
				
				while(it.hasNext()){
					sb.append(","+it.next());
				}
			}
			
			value = new Text(sb.toString());
			
			context.write(key, value);
		}
	}
	
	/**
	 * Reduce function for the initialization phase.
	 * 
	 * It receives the set of nodes' id that it is going to compose the adjacency list 
	 * for the node that the id is equals to the key. 
	 * It builds a Graphnode for each key, initializes it, and then writes it to the output.
	 * 	 * 
	 * @author Filippo Projetto
	 */
	public static class GetGraphReduce extends Reducer<Text, Text, Text, GraphNode>
	{
		/**
		 * @param key graph node's id
		 * @param values adjacent nodes' id
		 * @param context
		 */
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			/*
			 * Gets parameters passed through the context from the init function
			 */
			String srcId = context.getConfiguration().get("src");
			//String dstId = context.getConfiguration().get("dst");
			
			/*
			 * sb: the string that will be used to build the graph node.
			 * It contains the node id and its adjacency list.
			 */
			StringBuffer sb = new StringBuffer();

			sb.append(key+" ");
			Iterator<Text> it = values.iterator();
			
			if(it.hasNext()){
				sb.append(it.next());
				
				while(it.hasNext()){
					sb.append(","+it.next());
				}
			}
			
			/*
			 * Builds the graph node from the string we have just built
			 */
			GraphNode g = new GraphNode(sb.toString());
			
			/* Prepares data for the breadth search.
			*  If this is the source node, it will be the first to be visited.
			*  To do that we mark it with the gray color. 
			*/
			
			if(key.toString().compareTo(srcId) == 0){
				g.setDistance(0);
				g.setColor(Color.GRAY);
			}
			
			/*
			 * emit the node id and its graph representation
			 */
			context.write(key, g);
		}
	}
}
