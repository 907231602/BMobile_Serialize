package org.bf.pv.dimention;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class FlowNetCountValue implements Writable {

	private int upFlow;
	private int downFlow;
	private int totalFlow;
	private int count;
	
	public FlowNetCountValue(int upFlow, int downFlow, int count) {
		super();
		this.upFlow = upFlow;
		this.downFlow = downFlow;
		this.count = count;
		this.totalFlow=upFlow+downFlow;
	}

	public int getUpFlow() {
		return upFlow;
	}

	public void setUpFlow(int upFlow) {
		this.upFlow = upFlow;
	}

	public int getDownFlow() {
		return downFlow;
	}

	public void setDownFlow(int downFlow) {
		this.downFlow = downFlow;
	}

	public int getTotalFlow() {
		return totalFlow;
	}

	public void setTotalFlow(int totalFlow) {
		this.totalFlow = totalFlow;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(upFlow);
		out.writeInt(downFlow);
		out.writeInt(totalFlow);
		out.writeInt(count);

	}

	public void readFields(DataInput in) throws IOException {
		this.upFlow=in.readInt();
		this.downFlow=in.readInt();
		this.totalFlow=in.readInt();
		this.count=in.readInt();

	}


}
