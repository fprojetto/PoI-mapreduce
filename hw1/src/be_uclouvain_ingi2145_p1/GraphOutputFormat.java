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
import org.apache.log4j.Logger;


public class GraphOutputFormat extends FileOutputFormat<Text, GraphNode>{

	@Override
	public RecordWriter<Text, GraphNode> getRecordWriter(
			TaskAttemptContext job) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		FSDataOutputStream fileOut=null;
		String extension = "";
		
		Path file = getDefaultWorkFile(job, extension);
		
	    FileSystem fs = file.getFileSystem(job.getConfiguration());
	    try{
	    fileOut = fs.create(file, true);
	    }catch(IOException e){
	    	Logger.getRootLogger().fatal("[INGI2145] error while creating file");
	    	Logger.getRootLogger().fatal(e.getMessage());
	    	throw e;
	    }
	    
		return new GraphNodeRecordWriter(fileOut);
	}
	/*
	@Override
	public RecordWriter<LongWritable, GraphNode> getRecordWriter(
			FileSystem ignored, JobConf job, String name, Progressable progress)
			throws IOException {
		Path file = FileOutputFormat.getTaskOutputPath(job, name);
	    FileSystem fs = file.getFileSystem(job);
	    FSDataOutputStream fileOut = fs.create(file, progress);
	    
		return new GraphNodeRecordWriter(fileOut);
	}
	*/
	static class GraphNodeRecordWriter extends RecordWriter<Text, GraphNode>
	{
		private DataOutputStream out;
		
		public GraphNodeRecordWriter() {
			// TODO Auto-generated constructor stub
		}
		
		public GraphNodeRecordWriter(DataOutputStream out) throws IOException {
		      this.out = out;
		      
		    }
		
		@Override
		public synchronized void write(Text key, GraphNode value) throws IOException {
			// TODO Auto-generated method stub
		      if (key == null || value == null) {
		        return;
		      }
		      
		      out.writeBytes(key.toString()+" ");
		      
		      Iterator<Integer> it = value.edges.iterator();
		      
		      if(it.hasNext()){
		    	  out.writeBytes(""+it.next());
		      }
		      
		      while(it.hasNext()){
		    	  out.writeBytes(","+it.next());
		      }
		      //Logger.getRootLogger().fatal("[INGI2145] node written: "+value.edges);
		      if(value.distance == Integer.MAX_VALUE){
		    	  out.writeBytes(" MAX");
		      }else{
		    	  out.writeBytes(" "+value.distance);
		      }
		      out.writeBytes(" "+value.parentId);
		      out.writeBytes(" "+value.color);
		      out.write('\n');
		      }
/*
		@Override
		public synchronized void close(Reporter reporter) throws IOException {
			// TODO Auto-generated method stub
			
		}
*/
		@Override
		public synchronized void close(TaskAttemptContext context) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			out.close();
		}
		
	}



}
