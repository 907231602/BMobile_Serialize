package org.bf.pv.dimention;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class PVMobileDimention implements WritableComparable<PVMobileDimention>{

	private PVDimention pvDimention=new PVDimention();
	private String netAddress;
	private Integer type;
	
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		pvDimention.write(out);
		out.writeUTF(netAddress);
		out.writeInt(type);
	}

	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.pvDimention.readFields(in);
		netAddress=in.readUTF();
		type=in.readInt();
	}

	public int compareTo(PVMobileDimention o) {
		// TODO Auto-generated method stub
		if(this==o){
			return 0;
		}
		int tmp=pvDimention.compareTo(o.pvDimention);
				if(tmp!=0){
					return tmp;
				}
		
		tmp=netAddress.compareTo(o.netAddress);
		if(tmp!=0){
			return tmp;
		}
		tmp=type.compareTo(o.type);
		if(tmp!=0){
			return tmp;
		}
		return 0;
	}

	public PVDimention getPvDimention() {
		return pvDimention;
	}

	public void setPvDimention(PVDimention pvDimention) {
		this.pvDimention = pvDimention;
	}

	public String getNetAddress() {
		return netAddress;
	}

	public void setNetAddress(String netAddress) {
		this.netAddress = netAddress;
	}

	public Integer getType() {
		return type;
	}

	public void setType(Integer type) {
		this.type = type;
	}

}
