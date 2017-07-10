package org.bf.pv.runner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.bf.pv.dimention.PVDimention;
import org.bf.pv.dimention.PVFormat;
import org.bf.pv.mapper.PVMapper;
import org.bf.pv.reducer.PVReducer;

public class MainRun implements Tool {

	private Configuration conf=null;
	
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
		
		job.setMapperClass(PVMapper.class);
		job.setMapOutputKeyClass(PVDimention.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setReducerClass(PVReducer.class);
		job.setOutputKeyClass(PVDimention.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setOutputFormatClass(PVFormat.class);
		//FileOutputFormat.setOutputPath(job, new Path("hdfs://yanjijun1:9000/flow"));
		FileInputFormat.setInputPaths(job, new Path("hdfs://yanjijun1:9000/mobile.dat"));
		
		if(job.waitForCompletion(true)){
			return 1;
		}
		return 0;
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int i= ToolRunner.run(new MainRun(), args);
		System.out.println(i);

	}

}
