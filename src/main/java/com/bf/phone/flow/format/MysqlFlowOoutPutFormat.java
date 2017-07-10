package com.bf.phone.flow.format;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.bf.phone.flow.connetion.JdbcManager;
import com.bf.phone.flow.dimention.FlowDimention;
import com.bf.phone.flow.dimention.UpDownFlowDimention;

public class MysqlFlowOoutPutFormat extends OutputFormat<FlowDimention, UpDownFlowDimention> {

	@Override
	public void checkOutputSpecs(JobContext arg0) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public OutputCommitter getOutputCommitter(TaskAttemptContext arg0) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return new FileOutputCommitter(FileOutputFormat.getOutputPath(arg0),arg0);
	}

	@Override
	public RecordWriter<FlowDimention, UpDownFlowDimention> getRecordWriter(TaskAttemptContext context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		//获取hadoop配置信息
		Configuration conf=context.getConfiguration();
		
		Connection connection=JdbcManager.getConnection(conf);
		return new MysqlRecordWriter(conf,connection);
	}
	
      class MysqlRecordWriter extends RecordWriter<FlowDimention, UpDownFlowDimention>{
    	  private Configuration conf;
    	  private Connection con;
    	  private HashMap<String,PreparedStatement> psMaps=new HashMap<String,PreparedStatement>();
    	  public MysqlRecordWriter(Configuration conf,Connection con) {
			// TODO Auto-generated constructor stub
    		  this.con=con;
    		  this.conf=conf;
		}
    	  
		@Override
		public void close(TaskAttemptContext context) throws IOException, InterruptedException {
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
		private  int count=0;
		@Override
		public void write(FlowDimention key, UpDownFlowDimention value) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
		PreparedStatement ps=	psMaps.get("phone_flow");
		if (ps == null) {
			try {
				ps=con.prepareStatement(conf.get("phone_flow"));
				psMaps.put("phone_flow", ps);
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
		
		public void setPrepareStatement(PreparedStatement ps,FlowDimention key,UpDownFlowDimention value){
			try {
				ps.setString(1, key.getPhoneDate());
				ps.setString(2, key.getPhoneNumber());
				ps.setString(3, String.valueOf(value.getUpFlow()));
				ps.setString(4, String.valueOf(value.getDownFlow())  );
				ps.setString(5, String.valueOf(value.getTotalFlow()) );
				
				ps.setString(6, String.valueOf(value.getUpFlow()));
				ps.setString(7, String.valueOf(value.getDownFlow())  );
				ps.setString(8, String.valueOf(value.getTotalFlow()) );
				
				ps.addBatch();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
			
		
	}

}
