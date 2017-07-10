package com.bf.mobile.dimention;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class DatePhoneDimention implements WritableComparable<DatePhoneDimention> {

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
		out.writeUTF(phoneDate);
		out.writeUTF(phoneNumber);

	}

	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.phoneDate = in.readUTF();
		this.phoneNumber = in.readUTF();
	}

	public int compareTo(DatePhoneDimention arg0) {
		// TODO Auto-generated method stub
		if (this == arg0) {
			return 0;
		}
		int tmp = this.phoneDate.compareTo(arg0.phoneDate);
		if (tmp != 0) {
			return tmp;
		}
		tmp = this.phoneNumber.compareTo(arg0.phoneNumber);
		if (tmp != 0) {
			return tmp;
		}
		return 0;
	}

	@Override
	public int hashCode() {
		// TODO Auto-generated method stub
		int result = 1;
		int prime = 100;
		result = (result * prime) + this.phoneDate == null ? 0 : this.phoneDate.hashCode();
		result = (result * prime) + this.phoneNumber == null ? 0 : this.phoneNumber.hashCode();
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		DatePhoneDimention datePhoneDimention = (DatePhoneDimention) obj;
		if (this.phoneDate == null) {
			if (datePhoneDimention.getPhoneDate() != null) {
				return false;
			}
		} else if (this.phoneDate != null) {
			if (datePhoneDimention.getPhoneDate() == null) {
				return false;
			}
		} else if (!this.phoneDate.equals(datePhoneDimention.getPhoneDate())) {
			return false;
		}
		if (this.phoneNumber == null) {
			if (datePhoneDimention.getPhoneNumber() != null) {
				return false;
			}
		} else if (this.phoneNumber != null) {
			if (datePhoneDimention.getPhoneNumber() == null) {
				return false;
			}
		} else if (!this.phoneNumber.equals(datePhoneDimention.getPhoneNumber())) {
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
