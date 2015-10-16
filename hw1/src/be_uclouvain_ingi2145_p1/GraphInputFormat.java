package be_uclouvain_ingi2145_p1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import be_uclouvain_ingi2145_p1.GraphNode.Color;

public class GraphInputFormat extends FileInputFormat<LongWritable, GraphNode>{

	@Override
	public RecordReader<LongWritable, GraphNode> getRecordReader(
			InputSplit split, JobConf job, Reporter reporter)
					throws IOException {

		reporter.setStatus(split.toString());
		return new GraphNodeRecordReader(job, (FileSplit)split);
	}

	static class GraphNodeRecordReader implements RecordReader<LongWritable, GraphNode>{

		private LineRecordReader lineReader;
		private LongWritable lineKey;
		private Text lineValue;

		public GraphNodeRecordReader(JobConf job, FileSplit split) throws IOException {
			lineReader = new LineRecordReader(job, split);

			lineKey = lineReader.createKey();
			lineValue = lineReader.createValue();
		}

		public boolean next(LongWritable key, GraphNode value) throws IOException {
			// get the next line
			if (!lineReader.next(lineKey, lineValue)) {
				return false;
			}

			// parse the lineValue which is in the format:
			// objName, x, y, z
			//stringtokenizer is twice faster than split
			StringTokenizer st = new StringTokenizer(lineValue.toString(), " ");
			if (st.countTokens() != 6) {
				throw new IOException("Invalid record received");
			}
		     
			// try to parse 
			int id, distance, parentId;
			Color color;
			String edgesStringList = "";
			
			try {
				id = Integer.parseInt(st.nextToken().trim());
				edgesStringList = st.nextToken();
				distance = Integer.parseInt(st.nextToken().trim());
				parentId = Integer.parseInt(st.nextToken().trim());
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
					edges.add(Integer.parseInt(st.nextToken()));
				}catch(NumberFormatException nfe){
					throw new IOException("Error parsing integer value in record");
				}
			}

			// now that we know we'll succeed, overwrite the output objects
			value.id = id;
			value.distance = distance;
			value.parentId = parentId;
			value.color = color;
			value.edges = edges;		    

			key.set(id); // objName is the output key.

			return true;
		}

		public LongWritable createKey() {
			return new LongWritable();
		}

		public GraphNode createValue() {
			return new GraphNode();
		}

		public long getPos() throws IOException {
			return lineReader.getPos();
		}

		public void close() throws IOException {
			lineReader.close();
		}

		public float getProgress() throws IOException {
			return lineReader.getProgress();
		}

	}
}
