package com.bf.phone.flow.runner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bf.phone.flow.dimention.FlowDimention;
import com.bf.phone.flow.dimention.UpDownFlowDimention;
import com.bf.phone.flow.format.MysqlFlowOoutPutFormat;
import com.bf.phone.flow.mapper.FlowMapper;
import com.bf.phone.flow.reducer.FlowReducer;

public class PhoneFlowRunner implements Tool {

	private Configuration conf;
	public void setConf(Configuration conf) {
		// TODO Auto-generated method stub
		conf.addResource("jdbc_cfg.xml");
		conf.addResource("sql_mapper.xml");
		this.conf=conf;
		
	}

	public Configuration getConf() {
		// TODO Auto-generated method stub
		return conf;
	}

	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Job job=Job.getInstance(conf);
		job.setJarByClass(PhoneFlowRunner.class);
		
		job.setMapperClass(FlowMapper.class);
		job.setMapOutputKeyClass(FlowDimention.class);
		job.setOutputValueClass(UpDownFlowDimention.class);
		
		job.setReducerClass(FlowReducer.class);
		job.setOutputKeyClass(FlowDimention.class);
		job.setOutputValueClass(UpDownFlowDimention.class);
		
		job.setOutputFormatClass(MysqlFlowOoutPutFormat.class);
		//FileOutputFormat.setOutputPath(job, new Path("hdfs://yanjijun1:9000/flow"));
		FileInputFormat.setInputPaths(job, new Path("hdfs://yanjijun1:9000/mobile.dat"));
		
		if(job.waitForCompletion(true)){
			return 1;
		}
		return 0;
		
		
	}
	
	public static void main(String[] args) throws Exception {
	 int result=ToolRunner.run(new PhoneFlowRunner(), args);
	 System.out.println(result);
	}

}
