package com.bf.mobile.Main;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bf.mobile.Partitioner.MobilePartitioner;
import com.bf.mobile.dimention.FlowNetCountValue;
import com.bf.mobile.dimention.MobileDimention;
import com.bf.mobile.mapper.MobileMapper;
import com.bf.mobile.reducer.MobileReducer;

public class MobileRunner implements Tool {

	private Configuration conf;
	public void setConf(Configuration conf) {
		// TODO Auto-generated method stub
		this.conf=conf;

	}
	public Configuration getConf() {
		// TODO Auto-generated method stub
		return conf;
	}

	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(MobileRunner.class);
		job.setMapperClass(MobileMapper.class);
		job.setMapOutputKeyClass(MobileDimention.class);
		job.setMapOutputValueClass(FlowNetCountValue.class);
		job.setReducerClass(MobileReducer.class);
		job.setOutputKeyClass(MobileDimention.class);
		job.setOutputValueClass(FlowNetCountValue.class);
		
		job.setPartitionerClass(MobilePartitioner.class);
		job.setNumReduceTasks(2);
		FileOutputFormat.setOutputPath(job, new Path("hdfs://yanjijun1:9000/flowP"));
		FileInputFormat.setInputPaths(job, new Path("hdfs://yanjijun1:9000/mobile.dat"));
		if (job.waitForCompletion(true)) {
			return 1;
		}
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		int result=ToolRunner.run(new MobileRunner(), args);
		System.out.println(result);
	}

}
