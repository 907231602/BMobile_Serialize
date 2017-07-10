package org.number.dimention;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class NumDimention implements WritableComparable<NumDimention>{
	
	private String phoneDate;
	private String phoneNum;
	private String phoneWeb;
	public NumDimention(String phoneDate,String phoneNum,String phoneWeb) {
		// TODO Auto-generated constructor stub
		this.phoneDate=phoneDate;
		this.phoneNum=phoneNum;
		this.phoneWeb=phoneWeb;
		
	}
	
	public NumDimention() {
		// TODO Auto-generated constructor stub
	}
	
	
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
	public String getPhoneWeb() {
		return phoneWeb;
	}
	public void setPhoneWeb(String phoneWeb) {
		this.phoneWeb = phoneWeb;
	}
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		
		out.writeUTF(phoneDate);
		out.writeUTF(phoneNum);
		out.writeUTF(phoneWeb);
		
	}
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		
		phoneDate=in.readUTF();
		phoneNum=in.readUTF();
		phoneWeb=in.readUTF();
		
	}
	public int compareTo(NumDimention arg0) {
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
		tmp=this.phoneWeb.compareTo(arg0.phoneWeb);
		if (tmp!=0) {
			return tmp;
		}
		return 0;
		
	}

}
