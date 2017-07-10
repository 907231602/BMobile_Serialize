package com.bf.phone.flow.mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.bf.phone.flow.constants.DateType;
import com.bf.phone.flow.dimention.FlowDimention;
import com.bf.phone.flow.dimention.UpDownFlowDimention;
import com.bf.phone.flow.utils.DateUtils;

public class FlowMapper extends Mapper<LongWritable, Text, FlowDimention, UpDownFlowDimention> {

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, FlowDimention, UpDownFlowDimention>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		//super.map(key, value, context);
		
		String[] line=value.toString().split("##");
		System.out.println(line[0]);
		String date=DateUtils.toDate(line[0], DateType.DATE) ;
		String phoneNumber=line[1];
		String phoneUpFlow=line[8];
		String phoneDownFlow=line[9];
		
		FlowDimention flowDimention=new FlowDimention();
		flowDimention.setPhoneDate(date);
		flowDimention.setPhoneNumber(phoneNumber);

		UpDownFlowDimention upDownFlowDimention=new UpDownFlowDimention(Integer.parseInt(phoneUpFlow),Integer.parseInt(phoneDownFlow));
		
		context.write(flowDimention, upDownFlowDimention);
		
	}
}
