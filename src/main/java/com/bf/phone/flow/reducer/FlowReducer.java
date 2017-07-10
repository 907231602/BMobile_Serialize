package com.bf.phone.flow.reducer;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import com.bf.phone.flow.dimention.FlowDimention;
import com.bf.phone.flow.dimention.UpDownFlowDimention;

public class FlowReducer extends Reducer<FlowDimention, UpDownFlowDimention, FlowDimention, UpDownFlowDimention> {
@Override
protected void reduce(FlowDimention arg0, Iterable<UpDownFlowDimention> arg1,
		Reducer<FlowDimention, UpDownFlowDimention, FlowDimention, UpDownFlowDimention>.Context arg2)
		throws IOException, InterruptedException {
	// TODO Auto-generated method stub
	//super.reduce(arg0, arg1, arg2);
	
	int upFlow=0;
	int downFlow=0;
	for (UpDownFlowDimention upDown : arg1) {
		upFlow+=upDown.getUpFlow();
		downFlow+=upDown.getDownFlow();
	}
	UpDownFlowDimention upDownFlowDimention=new UpDownFlowDimention(upFlow, downFlow);
	arg2.write(arg0, upDownFlowDimention);
	
}
}
