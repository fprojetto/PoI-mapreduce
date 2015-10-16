package be_uclouvain_ingi2145_p1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Writable;

import org.apache.log4j.Logger;

public class GraphNode implements Writable{
	public static enum Color {
		WHITE, GRAY, BLACK
	};
	
	int id;
	int distance;
	int nEdges;
	List<Integer> edges;
	Color color;
	Integer parentId;

	public GraphNode()
	{
		this.id = 0;
		parentId = 0;
			edges = new ArrayList<Integer>();
			nEdges = 0;
			distance = Integer.MAX_VALUE;
			color = Color.WHITE;

	}
	
	public GraphNode(int id)
	{
		this.id = id;
		parentId = id;
			edges = new ArrayList<Integer>();
			nEdges = 0;
			distance = Integer.MAX_VALUE;
			color = Color.WHITE;

	}	
	// constructor
	//the parameter nodeInfo  is the line that is passed from the input, this nodeInfo is then split into key, value pair where the key is the node id
	//and the value is the information associated with the node
	
	public GraphNode(String nodeInfo) throws IOException {
		distance = Integer.MAX_VALUE;
		color = Color.WHITE;
		
		StringTokenizer st = new StringTokenizer(nodeInfo, " ");
		if (st.countTokens() != 2) {
			throw new IOException("Invalid record received");
		}
	     
		// try to parse 
		id = Integer.parseInt(st.nextToken());
		parentId = id;
		
		
		StringTokenizer stEdges = new StringTokenizer(st.nextToken(), ",");
		nEdges = stEdges.countTokens();
		
		edges = new ArrayList<Integer>();
		while(stEdges.hasMoreTokens()){
			try {
				edges.add(Integer.parseInt(stEdges.nextToken()));
			}catch(NumberFormatException nfe){
				throw new IOException("Error parsing integer value in record");
			}
		}		    			
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(id);
		out.writeInt(distance);
		out.writeInt(nEdges);
		
		for(Integer e : edges){
			out.writeInt(e);
		}
		out.writeUTF(color.toString());
		out.writeInt(parentId);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		id = in.readInt();
		distance = in.readInt();
		
		nEdges = in.readInt();
		
		edges = new ArrayList<Integer>();
		for(int i=0; i < nEdges; i++)
			edges.add(in.readInt());
		color = Color.valueOf(in.readUTF());
		parentId = in.readInt();
	}

}