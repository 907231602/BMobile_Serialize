package org.number.reducer;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.number.dimention.NumDimention;

public class NumReducer extends Reducer<NumDimention, IntWritable, NumDimention, IntWritable> {
 @Override
protected void reduce(NumDimention arg0, Iterable<IntWritable> arg1,
		Reducer<NumDimention, IntWritable, NumDimention, IntWritable>.Context arg2)
		throws IOException, InterruptedException {
	// TODO Auto-generated method stub
//	super.reduce(arg0, arg1, arg2);
	 
	 int count=0;
	 for (IntWritable intWritable : arg1) {
		count += intWritable.get();
	}
	 
	 arg2.write(arg0, new IntWritable(count));
	 	
}
}
