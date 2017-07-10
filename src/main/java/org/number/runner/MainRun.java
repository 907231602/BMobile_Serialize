package org.number.runner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.number.dimention.NumDimention;
import org.number.format.NumFormat;
import org.number.mapper.NumMapper;
import org.number.reducer.NumReducer;

import com.bf.phone.flow.format.MysqlFlowOoutPutFormat;

public class MainRun implements Tool{

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		 int result=ToolRunner.run(new MainRun(), args);
		 System.out.println(result);
	}

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
		job.setJarByClass(MainRun.class);
		
		job.setMapperClass(NumMapper.class);
		job.setMapOutputKeyClass(NumDimention.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setReducerClass(NumReducer.class);
		job.setOutputKeyClass(NumDimention.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setOutputFormatClass(NumFormat.class);
		//FileOutputFormat.setOutputPath(job, new Path("hdfs://yanjijun1:9000/flow"));
		FileInputFormat.setInputPaths(job, new Path("hdfs://yanjijun1:9000/mobile.dat"));
		
		if(job.waitForCompletion(true)){
			return 1;
		}
		return 0;
	}

}
