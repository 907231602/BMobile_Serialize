package org.bf.pv.dimention;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.bf.phone.flow.connetion.JdbcManager;



public class PVFormat extends OutputFormat<PVDimention, IntWritable> {

	@Override
	public RecordWriter<PVDimention, IntWritable> getRecordWriter(TaskAttemptContext context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf=context.getConfiguration();
		Connection con=JdbcManager.getConnection(conf);
		return new MyPVFormat(conf, con);
	}

	@Override
	public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return new FileOutputCommitter(FileOutputFormat.getOutputPath(context), context);
	}
	
	
	class MyPVFormat extends RecordWriter<PVDimention, IntWritable>{

		private Configuration conf;
		private Connection con;
		private HashMap<String,PreparedStatement> hash=new HashMap<String,PreparedStatement>();
		
		public MyPVFormat(Configuration conf,Connection con) {
			// TODO Auto-generated constructor stub
			this.conf=conf;
			this.con=con;
		}
		
		int count=0;
		@Override
		public void write(PVDimention key, IntWritable value) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			java.sql.PreparedStatement ps=hash.get("flow_pv");
			if(ps==null){
				try {
				ps= con.prepareStatement(conf.get("flow_pv"));
				hash.put("flow_pv", ps);
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			setPrepareStatement(ps,key,value);
			
			count++;
			if(count%10==0){
				try {
					ps.executeBatch();
					con.commit();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				count=0;
			}
		}

		public void setPrepareStatement(PreparedStatement ps,PVDimention key,IntWritable value){
			try {
				ps.setString(1, key.getPhoneDate());
				ps.setString(2, key.getPhoneNum());
				ps.setString(3, Integer.toString(value.get()));
				
				ps.setString(4, Integer.toString(value.get()));
				ps.addBatch();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			
			
		}
		
		@Override
		public void close(TaskAttemptContext context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			try {
			for (String entry : hash.keySet()) {
				
				PreparedStatement ps=hash.get(entry);
				System.out.println("--------------"+entry);
				ps.executeBatch();
				} 
				con.commit();
				
			}
			catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			finally{
				
				try {for (String key : hash.keySet()) {
				PreparedStatement ps;
				
					ps = con.prepareStatement(key);
					ps.close();
				} 
				}catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}finally{
					try {
						con.close();
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
			
			
		}
		
	}
	

}
