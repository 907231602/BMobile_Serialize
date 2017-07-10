package com.bf.mobile.mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import com.bf.mobile.constants.MobileConstants;
import com.bf.mobile.dimention.DatePhoneDimention;
import com.bf.mobile.dimention.FlowNetCountValue;
import com.bf.mobile.dimention.MobileDimention;
import com.bf.phone.flow.constants.DateType;
import com.bf.phone.flow.utils.DateUtils;

public class MobileMapper extends Mapper<LongWritable, Text, MobileDimention, FlowNetCountValue> {
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, MobileDimention, FlowNetCountValue>.Context context)
			throws IOException, InterruptedException {
		String line[] = value.toString().split("##");
		String times = line[0];
		String phoneNumber = line[1];
		String netAddress = line[4];
		String upFlow = line[8];
		String downFlow = line[9];
		// =======================做流量计算输出=========================================
		MobileDimention mobileDimention = new MobileDimention();
		DatePhoneDimention datePhoneDimention = new DatePhoneDimention();
		datePhoneDimention.setPhoneDate(DateUtils.toDate(times, DateType.DATE));
		datePhoneDimention.setPhoneNumber(phoneNumber);
		mobileDimention.setDatePhoneDimention(datePhoneDimention);
		FlowNetCountValue flowNetCountValue = new FlowNetCountValue(Integer.parseInt(upFlow),
				Integer.parseInt(downFlow), 0);	
		mobileDimention.setNetAddress("");
		mobileDimention.setType(MobileConstants.FLOWTYPE);
		context.write(mobileDimention, flowNetCountValue);
		// =======================做流量计算输出=========================================
		// =======================做网址计算输出=========================================
		if(netAddress!=null&&(!netAddress.equals("")))
		{
			mobileDimention.setNetAddress(netAddress);	
			mobileDimention.setType(MobileConstants.NETTYPE);
			flowNetCountValue.setCount(1);
			context.write(mobileDimention, flowNetCountValue);
		}
		// =======================做网址计算输出=========================================
		
		
	}
}
