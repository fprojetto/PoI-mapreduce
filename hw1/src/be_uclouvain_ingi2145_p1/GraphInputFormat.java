package be_uclouvain_ingi2145_p1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.log4j.Logger;

import be_uclouvain_ingi2145_p1.GraphNode.Color;

public class GraphInputFormat extends FileInputFormat<LongWritable, GraphNode>{

	@Override
	public RecordReader<LongWritable, GraphNode> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
			    
		return new GraphNodeRecordReader();
	}
	

	static class GraphNodeRecordReader extends RecordReader<LongWritable, GraphNode>{

		private LineRecordReader lineReader;
		private LongWritable lineKey;
		private Text lineValue;
		
		private GraphNode node;
		private LongWritable id;
				
		public GraphNodeRecordReader() {
			// TODO Auto-generated constructor stub
			lineReader = new LineRecordReader();
			lineKey = lineReader.getCurrentKey();
			lineValue = lineReader.getCurrentValue();
			
			node = new GraphNode();
			id = new LongWritable();
		}

		@Override
		public void initialize(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			lineReader.initialize(split, context);
		}


		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			if (!lineReader.nextKeyValue()) {
				return false;
			}
			
			
			lineValue = lineReader.getCurrentValue();
			lineKey = lineReader.getCurrentKey();
			
			// parse the lineValue which is in the format:
			// objName, x, y, z
			//stringtokenizer is twice faster than split
			StringTokenizer st = new StringTokenizer(lineValue.toString(), " ");
			if (st.countTokens() != 5) {
				throw new IOException("Invalid record received");
			}
		     
			// try to parse 
			int distance;
			Integer parentId, id;
			
			Color color;
			String edgesStringList = "";
			String tmp;
			
			try {
				id = Integer.parseInt(st.nextToken().trim());
				edgesStringList = st.nextToken();
				
				tmp = st.nextToken().trim();
				if(tmp.compareTo("MAX") == 0){
					distance = Integer.MAX_VALUE;
				}else{
					distance = Integer.parseInt(tmp);
				}
				
				tmp = st.nextToken().trim();
				if(tmp.compareTo("null") == 0){
					parentId = null;
				}else{
					parentId = Integer.parseInt(tmp);
				}
				color = Enum.valueOf(Color.class, st.nextToken().trim());
			} catch (NumberFormatException nfe) {
				throw new IOException("Error parsing integer value in record");
			} catch(IllegalArgumentException iae){
				throw new IOException("Error parsing enum Color value in record");
			}

			List<Integer> edges = new ArrayList<Integer>();

			
			StringTokenizer stEdges = new StringTokenizer(edgesStringList, ",");
			while(stEdges.hasMoreTokens()){
				try {
					edges.add(Integer.parseInt(stEdges.nextToken()));
				}catch(NumberFormatException nfe){
					throw new IOException("Error parsing integer value in record");
				}
			}

			// now that we know we'll succeed, overwrite the output objects
			node.id = id;
			node.distance = distance;
			node.parentId = parentId;
			node.color = color;
			node.edges = edges;		    
			node.nEdges = edges.size();
			
			this.id.set((long)id); // objName is the output key.

			
			
			return true;
		}


		@Override
		public LongWritable getCurrentKey() throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			return id;
		}


		@Override
		public GraphNode getCurrentValue() throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			return node;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			return lineReader.getProgress();
		}

		@Override
		public void close() throws IOException {
			// TODO Auto-generated method stub
			lineReader.close();
		}

	}

	

	
}
