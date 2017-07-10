package org.bf.pv.dimention;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class PVDimention implements WritableComparable<PVDimention>{
	private String phoneDate;
	private String phoneNum;
	public String getPhoneDate() {
		return phoneDate;
	}
	public void setPhoneDate(String phoneDate) {
		this.phoneDate = phoneDate;
	}
	public String getPhoneNum() {
		return phoneNum;
	}
	public void setPhoneNum(String phoneNum) {
		this.phoneNum = phoneNum;
	}
	public PVDimention() {
		// TODO Auto-generated constructor stub
	}

	public PVDimention(String phoneDate ,String phoneNum) {
		// TODO Auto-generated constructor stub
		this.phoneDate=phoneDate;
		this.phoneNum=phoneNum;
	}
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(phoneDate);
		out.writeUTF(phoneNum);
		
	}
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		phoneDate=in.readUTF();
		phoneNum=in.readUTF();
		
	}
	public int compareTo(PVDimention arg0) {
		// TODO Auto-generated method stub
		if(this==arg0){
			return 0;
		}
		int tmp=this.phoneDate.compareTo(arg0.phoneDate);
		if (tmp!=0) {
			return tmp;
		}
		tmp=this.phoneNum.compareTo(arg0.phoneNum);
		if (tmp!=0) {
			return tmp;
		}
		return 0;
	}
	
		@Override
	public int hashCode() {
		// TODO Auto-generated method stub
		int result=1;
		int prime=100;
		result=(result* prime)+this.phoneDate==null?0:this.phoneDate.hashCode();
		result=(result*prime)+this.phoneNum==null?0:this.phoneNum.hashCode();
		return result;
	}
		
		@Override
		public boolean equals(Object obj) {
			// TODO Auto-generated method stub
		if(this==obj){
			return true;
		}
		
		if(this.getClass()!= obj.getClass()){
			return false;
		}
		PVDimention pvDimention=(PVDimention) obj;
		
		if(this.phoneDate==null){
			if(pvDimention.phoneDate!=null){
				return false;
			}
		}
		
			
			return super.equals(obj);
		}
}
