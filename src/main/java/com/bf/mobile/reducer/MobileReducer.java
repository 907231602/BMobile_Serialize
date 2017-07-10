package com.bf.mobile.reducer;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;


import com.bf.mobile.constants.MobileConstants;
import com.bf.mobile.dimention.FlowNetCountValue;
import com.bf.mobile.dimention.MobileDimention;

public class MobileReducer extends Reducer<MobileDimention, FlowNetCountValue, MobileDimention, FlowNetCountValue> {
	@Override
	protected void reduce(MobileDimention key, Iterable<FlowNetCountValue> values,
			Reducer<MobileDimention, FlowNetCountValue, MobileDimention, FlowNetCountValue>.Context context)
			throws IOException, InterruptedException {
		
		if (key.getType() == MobileConstants.FLOWTYPE) {
			int upSum = 0;
			int downSum = 0;
			for (FlowNetCountValue fcv : values) {
				upSum += fcv.getUpFlow();
				downSum += fcv.getDownFlow();
			}
			FlowNetCountValue flowNetCountValue = new FlowNetCountValue(upSum, downSum, 0);
			context.write(key, flowNetCountValue);
		}
		if (key.getType() == MobileConstants.NETTYPE) {
			int count=0;
			for (FlowNetCountValue fcv : values) {
				count++;
			}
			FlowNetCountValue flowNetCountValue = new FlowNetCountValue(0, 0, count);
		    context.write(key, flowNetCountValue);
		}
		
		
	}
}
