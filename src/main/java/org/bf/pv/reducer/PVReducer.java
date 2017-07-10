package org.bf.pv.reducer;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.bf.pv.dimention.PVDimention;

public class PVReducer extends Reducer<PVDimention, IntWritable,PVDimention, IntWritable> {

	@Override
	protected void reduce(PVDimention arg0, Iterable<IntWritable> arg1,
			Reducer<PVDimention, IntWritable, PVDimention, IntWritable>.Context arg2)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
	//	super.reduce(arg0, arg1, arg2);
		int count=0;
		for (IntWritable intWritable : arg1) {
			count++;
		}
		
		arg2.write(arg0, new IntWritable(count));
		
	}
}
