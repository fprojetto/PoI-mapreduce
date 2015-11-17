package be_uclouvain_ingi2145_p1;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Custom OutputFormat that reads files of the particular format used to represent the graph.
 * Rather than implement OutputFormat directly, it subclass the FileOutputFormat. This
 * abstract class provides much of the basic handling necessary to manipulate files.
 * 
 * @author Filippo Projetto
 */

public class GraphOutputFormat extends FileOutputFormat<Text, GraphNode>{

	@Override
	public RecordWriter<Text, GraphNode> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException 
	{
		FSDataOutputStream fileOut = null;
		String extension = "";

		Path file = getDefaultWorkFile(job, extension);

		FileSystem fs = file.getFileSystem(job.getConfiguration());
		
		fileOut = fs.create(file, true);

		return new GraphNodeRecordWriter(fileOut);
	}
	
	static class GraphNodeRecordWriter extends RecordWriter<Text, GraphNode>
	{
		/*
		 * where the output data is written at the end of mapreduce job
		 */
		private DataOutputStream out;

		public GraphNodeRecordWriter(DataOutputStream out) throws IOException {
			this.out = out;
		}

		@Override
		public synchronized void write(Text key, GraphNode value) throws IOException {
			Iterator<Integer> it;
			
			if (key == null || value == null) {
				return;
			}

			//write node id
			out.writeBytes(key.toString()+" ");

			/*
			 * write neighborhood as id separated by comma
			 */
			it = value.getEdges().iterator();

			if(it.hasNext()){
				out.writeBytes(""+it.next());
				while(it.hasNext()){
					out.writeBytes(","+it.next());
				}
			}else{
				throw new IOException("Node " + key.toString() + "without neighboors: "+value.getEdges()+" "+Thread.currentThread().getStackTrace());
			}

			//write distance
			if(value.getDistance() == Integer.MAX_VALUE){
				out.writeBytes(" MAX");
			}else{
				out.writeBytes(" "+value.getDistance());
			}
			
			/*
			 * write list of first hop to reach this node from the source.
			 * node id separated by comma.
			 */
			it = value.getRoots().iterator();

			if(it.hasNext()){
				out.writeBytes(" "+it.next());
				while(it.hasNext()){
					out.writeBytes(","+it.next());
				}
			}else{
				out.writeBytes(" null");
			}
			
			//write color
			out.writeBytes(" "+value.getColor());
			
			//end of line
			out.writeBytes(System.lineSeparator());
		}
		
		@Override
		public synchronized void close(TaskAttemptContext context) throws IOException,
		InterruptedException {
			out.close();
		}
	}
}
