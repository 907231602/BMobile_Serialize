package com.bf.mobile.farmat;

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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.bf.mobile.collector.BaseCollector;
import com.bf.mobile.dimention.FlowNetCountValue;
import com.bf.mobile.dimention.MobileDimention;
import com.bf.phone.flow.connetion.JdbcManager;

public class MySqlMobileFormat extends OutputFormat<MobileDimention, FlowNetCountValue> {

	
	
	@Override
	public RecordWriter<MobileDimention, FlowNetCountValue> getRecordWriter(TaskAttemptContext context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf=context.getConfiguration();
		Connection con=JdbcManager.getConnection(conf);
		
		return new MysqlRecordWriter(conf, con);
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
	
	class MysqlRecordWriter extends RecordWriter<MobileDimention, FlowNetCountValue>{

		Connection con;
		Configuration conf;
		HashMap<String, PreparedStatement> psMaps=new HashMap<String, PreparedStatement>();
		public MysqlRecordWriter(Configuration conf,Connection con) {
			// TODO Auto-generated constructor stub
			this.conf=conf;
			this.con=con;
		}
		
		int count=0;
		@Override
		public void write(MobileDimention key, FlowNetCountValue value) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			PreparedStatement ps=psMaps.get("mobile"+key.getType());
			if(ps==null){
				try {
					ps=con.prepareStatement(conf.get("mobile"+key.getType()));
					
					psMaps.put("mobile"+key.getType(),ps);
					
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			count++;
			
			String classPath=conf.get("collector_"+key.getType());
			
			try {
				BaseCollector baseCollector=(BaseCollector) Class.forName(classPath).newInstance();
			baseCollector.setPreparestatement(ps, key, value);
			} catch (InstantiationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}

		@Override
		public void close(TaskAttemptContext context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			try {
				for (String key : psMaps.keySet()) {
					PreparedStatement ps = psMaps.get(key);
					ps.executeBatch();
				}
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				try {
					for (String key : psMaps.keySet()) {
						PreparedStatement ps = psMaps.get(key);
						ps.close();
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
				finally {
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
