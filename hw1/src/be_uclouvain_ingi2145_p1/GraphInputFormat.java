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

import be_uclouvain_ingi2145_p1.GraphNode.Color;

//TODO documentation

/**
 * Custom InputFormat that reads files of the particular format used to represent the graph.
 * Rather than implement InputFormat directly, it subclass the FileInputFormat. This
 * abstract class provides much of the basic handling necessary to manipulate files.
 * 
 * 
 * 
 * @author Filippo Projetto
 */
public class GraphInputFormat extends FileInputFormat<LongWritable, GraphNode>{
	@Override
	public RecordReader<LongWritable, GraphNode> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException 
	{
		return new GraphNodeRecordReader();
	}

	static class GraphNodeRecordReader extends RecordReader<LongWritable, GraphNode>{
		private LineRecordReader lineReader;
		@SuppressWarnings("unused")
		private LongWritable lineKey;
		private Text lineValue;

		private GraphNode node;
		private LongWritable id;

		public GraphNodeRecordReader() {
			
			lineReader = new LineRecordReader();
			lineKey = lineReader.getCurrentKey();
			lineValue = lineReader.getCurrentValue();

			node = new GraphNode();
			id = new LongWritable();
		}

		@Override
		public void initialize(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
			lineReader.initialize(split, context);
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			// try to parse 
			int distance;
			Integer id;
			List<Integer> roots;
			List<Integer> edges;
			Color color;
			String edgesStringList, rootsStringList, distanceString;
			StringTokenizer stEdges, stRoots, stLine;
			
			if (!lineReader.nextKeyValue()) {
				return false;
			}

			lineValue = lineReader.getCurrentValue();
			lineKey = lineReader.getCurrentKey();

			stLine = new StringTokenizer(lineValue.toString(), " ");
			
			if (stLine.countTokens() != 5) {
				throw new IOException("Invalid record received: "+lineValue.toString());
			}

			try {
				id = Integer.parseInt(stLine.nextToken().trim());
				edgesStringList = stLine.nextToken();
				distanceString = stLine.nextToken().trim();
				
				edges = new ArrayList<Integer>();
				stEdges = new StringTokenizer(edgesStringList, ",");
				while(stEdges.hasMoreTokens()){
						edges.add(Integer.parseInt(stEdges.nextToken()));
				}
				
				if(distanceString.compareTo("MAX") == 0){
					distance = Integer.MAX_VALUE;
				}else{
					distance = Integer.parseInt(distanceString);
				}
				
				rootsStringList = stLine.nextToken().trim();
				
				roots = new ArrayList<Integer>();
				if(rootsStringList.compareTo("null") != 0){
					stRoots = new StringTokenizer(rootsStringList, ",");
					while(stRoots.hasMoreTokens()){
							roots.add(Integer.parseInt(stRoots.nextToken()));
					}
				}
				
				color = Enum.valueOf(Color.class, stLine.nextToken().trim());
			} catch (NumberFormatException nfe) {
				throw new IOException("Error parsing integer value in record");
			} catch(IllegalArgumentException iae){
				throw new IOException("Error parsing enum Color value in record");
			}

			// now that we know we'll succeed, overwrite the output objects
			node.setId(id);
			node.setDistance(distance);
			node.setRoots(roots);
			node.setColor(color);
			node.setEdges(edges);

			this.id.set((long)id); // objName is the output key.

			return true;
		}


		@Override
		public LongWritable getCurrentKey() throws IOException, InterruptedException 
		{
			return id;
		}


		@Override
		public GraphNode getCurrentValue() throws IOException, InterruptedException 
		{
			return node;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return lineReader.getProgress();
		}

		@Override
		public void close() throws IOException {
			lineReader.close();
		}
	}
}
