package org.bf.pv.mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.bf.pv.dimention.FlowNetCountValue;
import org.bf.pv.dimention.PVMobileDimention;

public class MobileMapper extends Mapper<LongWritable, Text, PVMobileDimention, FlowNetCountValue> {
@Override
protected void map(LongWritable key, Text value,
		Mapper<LongWritable, Text, PVMobileDimention, FlowNetCountValue>.Context context)
		throws IOException, InterruptedException {
	// TODO Auto-generated method stub
	//super.map(key, value, context);
	String line[] = value.toString().split("##");
	String times = line[0];
	String phoneNumber = line[1];
	String netAddress = line[4];
	String upFlow = line[8];
	String downFlow = line[9];
	// =======================做流量计算输出=========================================
	
	// =======================做流量计算输出=========================================

}
}
