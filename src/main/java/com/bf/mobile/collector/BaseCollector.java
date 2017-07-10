package com.bf.mobile.collector;

import java.sql.PreparedStatement;

import com.bf.mobile.dimention.FlowNetCountValue;
import com.bf.mobile.dimention.MobileDimention;

public interface BaseCollector {

	public void setPreparestatement(PreparedStatement ps,MobileDimention key, FlowNetCountValue value);
}
