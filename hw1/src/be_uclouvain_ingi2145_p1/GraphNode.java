package be_uclouvain_ingi2145_p1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Writable;

public class GraphNode implements Writable{
	public static enum Color {
		WHITE, GRAY, BLACK
	};
	
	int id;
	int distance;
	int nEdges;
	List<Integer> edges = new ArrayList<Integer>();
	Color color;
	Integer parentId;

	public GraphNode()
	{

			
			
			distance = Integer.MAX_VALUE;
			color = Color.WHITE;
			parentId = null;

		
	}
	
	// constructor
	//the parameter nodeInfo  is the line that is passed from the input, this nodeInfo is then split into key, value pair where the key is the node id
	//and the value is the information associated with the node
	
	public GraphNode(String nodeInfo) throws IOException {
		this();
		
		StringTokenizer st = new StringTokenizer(nodeInfo, " ");
		if (st.countTokens() != 2) {
			throw new IOException("Invalid record received");
		}
	     
		// try to parse 
		
		id = Integer.parseInt(st.nextToken());
		
		StringTokenizer stEdges = new StringTokenizer(st.nextToken(), ",");
		nEdges = stEdges.countTokens();
		
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
		out.write(id);
		out.write(distance);
		for(Integer e : edges){
			out.write(e);
		}
		out.writeUTF(color.toString());
		out.write(parentId);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		id = in.readInt();
		distance = in.readInt();
		for(int i=0; i < nEdges; i++)
			edges.add(in.readInt());
		color = Color.valueOf(in.readUTF());
		parentId = in.readInt();
	}

}
