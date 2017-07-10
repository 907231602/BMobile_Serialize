package org.number.mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.number.dimention.NumDimention;

import com.bf.phone.flow.constants.DateType;
import com.bf.phone.flow.utils.DateUtils;

public class NumMapper extends Mapper<LongWritable, Text, NumDimention, IntWritable> {
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, NumDimention, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		//super.map(key, value, context);
		
		String[] values= value.toString().split("##");
		String phoneDate= DateUtils.toDate(values[0], DateType.DATE) ;
		String phoneNum=values[1];
		String phoneWeb=values[4];
		
		if(phoneWeb.equals("")){
			;
		}else{
			NumDimention numDimention= new NumDimention(phoneDate,phoneNum,phoneWeb);
			System.out.println(numDimention.getPhoneWeb());
			context.write(numDimention, new IntWritable(1));
		}
		
		
		
	}
}
