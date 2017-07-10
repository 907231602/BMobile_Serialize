package com.bf.mobile.dimention;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class MobileDimention implements WritableComparable<MobileDimention> {

	private DatePhoneDimention datePhoneDimention = new DatePhoneDimention();;

	private String netAddress;
     //用于表种统计种类
	private Integer type;

	public DatePhoneDimention getDatePhoneDimention() {
		return datePhoneDimention;
	}

	public void setDatePhoneDimention(DatePhoneDimention datePhoneDimention) {
		this.datePhoneDimention = datePhoneDimention;
	}

	public String getNetAddress() {
		return netAddress;
	}

	public void setNetAddress(String netAddress) {
		this.netAddress = netAddress;
	}

	public int getType() {
		return type;
	}

	public void setType(int type) {
		this.type = type;
	}

	public void write(DataOutput out) throws IOException {

		datePhoneDimention.write(out);
		out.writeUTF(netAddress);
		out.writeInt(type);

	}

	public void readFields(DataInput in) throws IOException {
		this.datePhoneDimention.readFields(in);
		netAddress = in.readUTF();
		type = in.readInt();
	}

	public int compareTo(MobileDimention arg0) {

		if (this == arg0) {
			return 0;
		}
		int tmp = datePhoneDimention.compareTo(arg0.datePhoneDimention);
		if (tmp != 0) {
			return tmp;
		}
		tmp = netAddress.compareTo(arg0.netAddress);
		if (tmp != 0) {
			return tmp;
		}
		tmp = type.compareTo(arg0.type);
		if (tmp != 0) {
			return tmp;
		}
		return 0;
	}

	/**
	 * 重写hashcode方法
	 */
	@Override
	public int hashCode() {
		int result = 1;
		int prime = 100;
		result = (result * prime) + (datePhoneDimention == null ? 0 : datePhoneDimention.hashCode());
		result = (result * prime) + (netAddress == null ? 0 : netAddress.hashCode());
		result = (result * prime) + (type == null ? 0 : type.hashCode());
		return result;
	}

	/**
	 * 重写equals方法
	 */
	@Override
	public boolean equals(Object arg0) {
		if (this == arg0) {
			return true;
		}
		if (getClass() != arg0.getClass()) {
			return false;
		}
		MobileDimention mobileDimention = (MobileDimention) arg0;
		//=================================判断日期和电话维度是否相同==================
		if (this.datePhoneDimention == null) {
			if (mobileDimention.datePhoneDimention != null) {
				return false;
			}
		} else if (this.datePhoneDimention != null) {
			if (mobileDimention.datePhoneDimention == null) {
				return false;
			}
		} else if (!this.datePhoneDimention.equals(mobileDimention.datePhoneDimention)) {
			return false;
		}
		//=================================判断日期和电话维度是否相同==================
		if (netAddress == null) {
			if (mobileDimention.netAddress != null) {
				return false;
			}
		} else if (netAddress != null) {
			if (mobileDimention.netAddress == null) {
				return false;
			}
		} else if (!netAddress.equals(mobileDimention.netAddress)) {
			return false;
		}
		if (type == null) {
			if (mobileDimention.type != null) {
				return false;
			}
		} else if (type != null) {
			if (mobileDimention.type == null) {
				return false;
			}
		} else if (!type.equals(type)) {
			return false;
		}

		return true;
	}
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return this.datePhoneDimention.toString()+"\t"+netAddress+"\t"+type;
	}


}
