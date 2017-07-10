package com.bf.phone.flow.dimention;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;


/**
 * 如果序列化当key,一定要实现WritableComparable
 * @author Administrator
 * 序列化
 * 纬度类
 *
 */
public class FlowDimention implements WritableComparable<FlowDimention> {

	private String phoneDate;
	private String phoneNumber;
	public String getPhoneDate() {
		return phoneDate;
	}

	public void setPhoneDate(String phoneDate) {
		this.phoneDate = phoneDate;
	}

	public String getPhoneNumber() {
		return phoneNumber;
	}
	public void setPhoneNumber(String phoneNumber) {
		this.phoneNumber = phoneNumber;
	}
	
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(this.phoneDate);
		out.writeUTF(this.phoneNumber);
		
	}
	

	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		
		this.phoneDate=in.readUTF();
		this.phoneNumber=in.readUTF();
	}

	

	public int compareTo(FlowDimention arg0) {
		System.out.println("______________----compareTO");
		// TODO Auto-generated method stub
		if(this==arg0){
			return 0;
		}
		int tmp=this.phoneDate.compareTo(arg0.phoneDate);
		if (tmp!=0) {
			return tmp;
		}
		tmp=this.phoneNumber.compareTo(arg0.phoneNumber);
		if (tmp!=0) {
			return tmp;
		}
		return 0;
	}
	
	@Override
	public int hashCode() {
		// TODO Auto-generated method stub
		//return super.hashCode();
		int result=1;
		int prime=100;
		result=(result*prime)+phoneDate==null?0:phoneDate.hashCode();
		result=(result*prime)+phoneNumber==null?0:phoneNumber.hashCode();
		return result;
		
	}
	
	@Override
	public boolean equals(Object obj) {
		// TODO Auto-generated method stub
	//	return super.equals(obj);
		if(this==obj){
			return true;
			
		}
		if(getClass()!=obj.getClass()){
			return false;
		}
		
		FlowDimention flowDimention=(FlowDimention) obj;
		if(this.phoneDate==null){
			if(flowDimention.phoneDate!=null){
				return false;
			}
			
		}else if(this.phoneDate!=null){
			if(flowDimention.phoneDate==null){
				return false;
			}
		}
		else if (!this.phoneDate.equals(flowDimention.phoneDate)) {
			return false;
		}
		
		
		//
		if(this.phoneNumber==null){
			if(flowDimention.phoneNumber!=null){
				return false;
			}
			
		}else if(this.phoneNumber!=null){
			if(flowDimention.phoneNumber==null){
				return false;
			}
		}
		else if (!this.phoneNumber.equals(flowDimention.phoneNumber)) {
			return false;
		}
		
		return true;
		
	}
	
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return this.phoneDate+"\t"+this.phoneNumber;
	}
	
	

}
