package com.bf.mobile.collector;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.bf.mobile.dimention.FlowNetCountValue;
import com.bf.mobile.dimention.MobileDimention;

public class MoblieNetCountCollecto implements BaseCollector {

	public void setPreparestatement(PreparedStatement ps, MobileDimention key, FlowNetCountValue value) {
		// TODO Auto-generated method stub
		try {
			ps.setString(1,key.getDatePhoneDimention().getPhoneDate() );
			ps.setString(2, key.getDatePhoneDimention().getPhoneNumber());
			ps.setString(3, String.valueOf(value.getCount()));
			ps.setString(4, String.valueOf(value.getCount()));
			ps.addBatch();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}

}
