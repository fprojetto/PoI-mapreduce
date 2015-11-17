package be_uclouvain_ingi2145_p1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Writable;

/** 
 * GraphNode class.
 * It represents one node in the social graph. 
 * It carries information about it's neighborhood and info used for the PoI algorithm.
 * 
 * It implements the Writable interface so it can be emitted as a type, without needs of 
 * conversions from/to a string for example, through various steps of a map reduce job. 
 * 
 * @author Filippo Projetto
 */
public class GraphNode implements Writable{
	
	/**
	 * node id
	 */
	private int id; 
	/**
	 * minimum distance from source node
	 */
	private int distance; 
	/**
	 * neighborhood: list of nodes id
	 */
	private List<Integer> edges;
	/**
	 * represents the status of the node during the breadth search
	 */
	private Color color;
	/**
	 * First hop to reach this node from the source, with the minimum cost. Eventually more than one.
	 * It is used to give in output the set of immediate neighbors of the source node that have been used  
	 * to PoIs found by the algorithm.
	 */
	private List<Integer> roots; 
	
	/**
	 * Build a graph node without any information.
	 * 
	 * Set distance to the source as infinite (MAX_VALUE) and mark it as not visited/examined
	 * yet (WHITE).
	 * 
	 */
	public GraphNode()
	{
		this.id = 0;
		roots = new ArrayList<Integer>();
		edges = new ArrayList<Integer>();
		distance = Integer.MAX_VALUE;
		color = Color.WHITE;
	}

	/**
	 * Build a graph node without information about its connection on the graph.
	 * Set distance to the source as infinite (MAX_VALUE) and mark it as not 
	 * visited/examined yet (WHITE).
	 * 
	 * @param id node id
	 */
	public GraphNode(int id)
	{
		this();
		this.id = id;
	}	
	
	/**
	 * Build a graph node setting its id and neighbor according to the input parameter.
	 * Set distance to the source as infinite (MAX_VALUE) and mark it as not 
	 * visited/examined yet (WHITE).
	 * 
	 * @param nodeInfo node id and list of neighbors
	 * @throws IOException
	 */
	public GraphNode(String nodeInfo) throws IOException 
	{
		this();
		
		StringTokenizer stEdges;
		StringTokenizer st;
		
		/*
		 * parsify the string according to the format (<node id> <[list of neighbors]>)
		 */
		st = new StringTokenizer(nodeInfo, " ");
		if (st.countTokens() != 2) {
			throw new IOException("Invalid record received");
		}

		try {
			id = Integer.parseInt(st.nextToken());// set id
	
			/*
			 * Set the neighborhood
			 */
			stEdges = new StringTokenizer(st.nextToken(), ",");
			while(stEdges.hasMoreTokens()){
					edges.add(Integer.parseInt(stEdges.nextToken()));
			}	
		}catch(NumberFormatException nfe){
			throw new IOException("Error parsing integer value in record");
		}
	}
	
	/**
	 * 
	 * During the search, a node can be in one of this states:
	 * 		- WHITE, not visited yet
	 * 		- GRAY, just visited, its neighborhood it is going to be visited in the next iteration if any
	 * 		- BLACK, done. No more processing on its neighborhood.
	 * 
	 * @author Filippo Projetto
	 *
	 */
	public static enum Color {
		WHITE, GRAY, BLACK
	};

	/**
	 * 
	 * @return node id
	 */
	public int getId() {
		return id;
	}

	/**
	 * 
	 * @param id node id
	 */
	public void setId(int id) {
		this.id = id;
	}

	/**
	 * 
	 * @return estimated distance to the source node. It is equal to MAX_VALUE at the beginning.
	 */
	public int getDistance() {
		return distance;
	}

	/**
	 * 
	 * @param distance set distance to the source node.
	 */
	public void setDistance(int distance) {
		this.distance = distance;
	}

	/**
	 * 
	 * @return list of neighbors' id
	 */
	public List<Integer> getEdges() {
		return edges;
	}

	/**
	 * 
	 * set the list of neighbors
	 */
	public void setEdges(List<Integer> edges) {
		this.edges = new ArrayList<Integer>(edges);
	}

	/**
	 * 
	 * @return color = state during the search (WHITE, GRAY, BLACK).
	 */
	public Color getColor() {
		return color;
	}

	/**
	 * 
	 * set the color according to the node state during the search (WHITE, GRAY, BLACK).
	 */
	public void setColor(Color color) {
		this.color = color;
	}

	/**
	 * Add <roots> to the list of neighbors for this node.
	 * 
	 * @param roots immediate source node neighbors
	 */
	public void addRoots(List<Integer> roots) 
	{
		roots.addAll(roots);
	}
	
	/**
	 * 
	 * @return the list of immediate source node neighbors for this node.
	 */
	public List<Integer> getRoots() 
	{
		return roots;
	}
	
	/**
	 * Create a new immediate source node neighbors list
	 * initializing it with the list passed as argument
	 * 
	 * @param roots immediate source node neighbors
	 */
	public void setRoots(List<Integer> roots) 
	{
		this.roots = new ArrayList<Integer>(roots);
	}
	
	/**
	 * Add <root> to the list of neighbors for this node.
	 * 
	 * @param root immediate source node neighbor
	 */
	public void addRoot(Integer root) 
	{
		this.roots.add(root);
	}
	
	/**
	 * implementation of the Writeble interface to make this type
	 * usable as input of Hadoop mapper, reducer, ... 
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(id);
		out.writeInt(distance);
		
		/**
		 * write neighborhood and its size.
		 * The size is only used to know how integers read
		 * in the readFields function.
		 */
		out.writeInt(edges.size());
		for(Integer e : edges){
			out.writeInt(e);
		}

		out.writeUTF(color.toString());
		
		/**
		 * write immediate neighbors of the  
		 * source node and its size.
		 * The size is only used to know how integers read
		 * in the readFields function.
		 */
		out.writeInt(roots.size());
		if(roots.size() > 0){	
			for(Integer i : roots)
				out.writeInt(i);
		}
	}

	/**
	 * implementation of the Writeble interface to make this type
	 * usable as input of Hadoop mapper, reducer, ... 
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		int nEdges;
		int nRoots=0;
		
		id = in.readInt();
		distance = in.readInt();

		/*
		 * read adjacency list
		 */
		nEdges = in.readInt();
		edges = new ArrayList<Integer>();
		for(int i=0; i < nEdges; i++)
			edges.add(in.readInt());
		
		color = Color.valueOf(in.readUTF());
		
		/*
		 * read immediate neighbors of the  
		 * source node list
		 */
		nRoots = in.readInt();
		roots = new ArrayList<Integer>();
		for(int j =0; j < nRoots; j++){
			roots.add(in.readInt());
		}

	}
}