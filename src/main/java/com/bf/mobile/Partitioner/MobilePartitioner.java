package com.bf.mobile.Partitioner;

import org.apache.hadoop.mapreduce.Partitioner;

import com.bf.mobile.dimention.FlowNetCountValue;
import com.bf.mobile.dimention.MobileDimention;

public class MobilePartitioner extends Partitioner<MobileDimention, FlowNetCountValue> {

	@Override
	public int getPartition(MobileDimention key, FlowNetCountValue value, int numPartitions) {
		// TODO Auto-generated method stub
		return key.getType()==1?0:1;
	}

}
