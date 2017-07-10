package org.number.format;

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
import org.number.dimention.NumDimention;

import com.bf.phone.flow.connetion.JdbcManager;
import com.bf.phone.flow.dimention.FlowDimention;
import com.bf.phone.flow.dimention.UpDownFlowDimention;

public class NumFormat extends OutputFormat<NumDimention, IntWritable> {

	@Override
	public RecordWriter<NumDimention, IntWritable> getRecordWriter(TaskAttemptContext context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		Connection con = JdbcManager.getConnection(conf);
		System.out.println("--------------------RecordWriter");
		return new MysqlNumRecordWriter(conf, con);
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
	
	class MysqlNumRecordWriter extends RecordWriter<NumDimention, IntWritable>{
		
		private Configuration conf;
  	  private Connection con;
  	  private HashMap<String,PreparedStatement> psMaps=new HashMap<String,PreparedStatement>();
  	   public MysqlNumRecordWriter(Configuration conf,Connection con) {
			// TODO Auto-generated constructor stub
  		  this.con=con;
  		  this.conf=conf;
  		  System.out.println("-----------------------MysqlNUm");
		}
		
  	   int count=0;
		@Override
		public void write(NumDimention key, IntWritable value) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			PreparedStatement ps=psMaps.get("flow_Num");
			if (ps == null) {
				try {
					ps=con.prepareStatement(conf.get("flow_Num"));
					psMaps.put("flow_Num", ps);
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			//为ps赋值
			setPrepareStatement(ps, key, value);
			//批量执行sql
			count++;
			if (count%10==0) {
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
		
		public void setPrepareStatement(PreparedStatement ps,NumDimention key,IntWritable value){
			try {
				ps.setString(1, key.getPhoneDate());
				ps.setString(2, key.getPhoneNum());
				ps.setString(3, key.getPhoneWeb());
				ps.setString(4, Integer.toString(value.get()) );
				
				ps.setString(5, Integer.toString(value.get())  );
				
				
				
				ps.addBatch();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}

		@Override
		public void close(TaskAttemptContext context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub

			// TODO Auto-generated method stub
			try {
			for (String psKey : psMaps.keySet()) {
				PreparedStatement ps=psMaps.get(psKey);
					ps.executeBatch();
			}
			con.commit();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}finally{
				try {
					for (String psKey : psMaps.keySet()) {
						PreparedStatement ps=psMaps.get(psKey);
							ps.close();
					}
					} catch (SQLException e) {
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
