package be_uclouvain_ingi2145_p1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import be_uclouvain_ingi2145_p1.GraphNode.Color;

/**
 * Implementation of the parallel breadth search algorithm to solve the PoI problem.
 * 
 * @author Filippo Projetto
 *
 */
public class GraphSearch {
	
	/**
	 * Map function for the iter phase.
	 * 
	 * Each iteration of the algorithm expands the "search frontier" by one hop
	 * examining the adjacency list for the gray nodes.
	 * 
	 * @author Filippo Projetto
	 */
	public static class GraphSearchMap extends Mapper<LongWritable, GraphNode, Text, GraphNode>
	{   
		/**
		 * stores adjacency list nodes' id during the computation
		 */
		private Text neighborId = new Text();
		/**
		 * current node id
		 */
		private Text nodeId = new Text();

		/**
		 * @param key line number in the text file
		 * @param value GraphNode object
		 * @param context
		 * @throws IOException
		 * @throws InterruptedException
		 */
		@Override
		protected void map(LongWritable key, GraphNode value, Context context) throws IOException, InterruptedException
		{
			final int srcId = Integer.parseInt(context.getConfiguration().get("src"));
			GraphNode adjacentNode;
			
			if (value.getColor() == Color.GRAY){
				for(Integer neighbor : value.getEdges()){ //for all the adjacent nodes of the gray node
					adjacentNode = new GraphNode(neighbor); //create a new node passing the id
					adjacentNode.setDistance(value.getDistance() +1); //set the distance of the node as 1 unit more than the gray node
					adjacentNode.setColor(Color.GRAY); //set the color of the node to be GRAY, it will be explored at the next step if we have not did it yet

					if(value.getId() == srcId){
						adjacentNode.addRoot(neighbor);
					}else{
						adjacentNode.setRoots(value.getRoots());
					}
					/*
					 * emit the information about the adjacent node in the form of key-value pair where the key 
					 * is the node Id and the value is the associated information for the nodes emitted here,
					 * the list of adjacent nodes will be empty in the value part, the reducer will merge this 
					 * with the original list of adjacent nodes associated with a node
					*/
					neighborId.set(neighbor.toString());
					context.write(neighborId, adjacentNode);
				}
				// this node is done, color it black
				value.setColor(Color.BLACK);
			}
			/* No matter what, emit the input node
			 * If the node came into this method GRAY, it will be output as BLACK
			 * otherwise, the nodes that are already colored BLACK or WHITE are emitted by setting the key 
			 * as the node Id and the value as the node's information
			*/
			
			nodeId.set(""+value.getId());
			context.write(nodeId, value);
		}
	}

	/**
	 * Reduce function for the iter phase.
	 * 
	 * Updates the graph nodes parameters (cost to reach the source, color, immediate neighbor(s)) 
	 * according to the new information coming from the map phase. 
	 * It receives multiple Graphnodes for the nodes belonging at the adjacency list of the nodes 
	 * examined in the map phase, and emits only one Graphnode structure for each node id.
	 * 
	 * @author Filippo Projetto
	 */
	public static class GraphSearchReduce extends Reducer<Text, GraphNode, Text, GraphNode>
	{		
		private GraphNode outnode;
		/**
		 * @param key node id
		 * @param values GraphNode object list representing various instance for the same node, 
		 * the one with <key> node id, with different path cost estimation 
		 * @param context
		 * @throws IOException
		 * @throws InterruptedException 
		 */
		@Override
		protected void reduce(Text key, Iterable<GraphNode> values, Context context) throws IOException, InterruptedException
		{
			/**
			 * node that will be emitted, for that particular key, at the end of reduce phase
			 */
			outnode = new GraphNode(Integer.parseInt(key.toString()));

			/*
			 * Scan all possible path cost for the same node and pick 
			 * the best one, that means the one less distant from the source.
			 */
			for(GraphNode curr : values){
				if (curr.getDistance() < outnode.getDistance()) {
					/*
					 * discovered a better path.
					 * Set the distance appropriately and add the immediate neighbors 
					 * of the source node in the appropriate set.
					 */
					outnode.setDistance(curr.getDistance());
					outnode.setRoots(curr.getRoots());
				}else if(curr.getDistance() == outnode.getDistance()){
					/*
					 * two different paths from the source node to this node at the same cost.
					 * add the immediate neighbors of the source node in the appropriate set.
					 */
					outnode.addRoots(curr.getRoots());
				}
				
				//set the neighborhood for the output node. Do this only once.
				if(outnode.getEdges().size() == 0 && curr.getEdges().size() != 0){
					outnode.setEdges(curr.getEdges());
				}
				
				//get the more updated value for the node state
				if(curr.getColor().ordinal() > outnode.getColor().ordinal()){
					outnode.setColor(curr.getColor());
				}
			}
			
			//emit the key, value pair where the key is the node id and the value is the node's information
			context.write(key, outnode);		
		}
	}
}
