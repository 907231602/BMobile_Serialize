package org.bf.pv.mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.bf.pv.dimention.PVDimention;

import com.bf.phone.flow.constants.DateType;
import com.bf.phone.flow.utils.DateUtils;

public class PVMapper extends Mapper<LongWritable, Text, PVDimention, IntWritable> {

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, PVDimention, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		//super.map(key, value, context);
		String[] values=value.toString().split("##");
		
		String phoneDate=DateUtils.toDate(values[0], DateType.DATE) ;
		String phoneNum=values[1];
		String phoneWeb=values[4];
		if(phoneWeb.equals("")){
			
		}else{
			PVDimention pvDimention=new PVDimention(phoneDate, phoneNum);
			
			context.write(pvDimention, new IntWritable(1));
		}
		
	
		
	}
	
	
}
