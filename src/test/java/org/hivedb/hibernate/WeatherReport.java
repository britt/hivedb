package org.hivedb.hibernate;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.Random;

import org.hivedb.hibernate.annotations.EntityId;
import org.hivedb.hibernate.annotations.Index;
import org.hivedb.hibernate.annotations.PartitionIndex;
import org.hivedb.hibernate.annotations.Resource;
import org.hivedb.util.HiveUtils;

@Resource(name="WeatherReport")
public interface WeatherReport {
	
	public static final String REPORT_ID = "reportId";
	public static final String TEMPERATURE = "temperature";
	public static final String CONTINENT = "continent";
    
    @PartitionIndex(name=WeatherReport.CONTINENT)
	String getContinent();
	BigDecimal getLatitude();
	BigDecimal getLongitude();
	
	@EntityId
	Integer getReportId();
	Date getReportTime();

	@Index
	int getTemperature();
	
	@Index
	Collection<Integer> getCollectionIndex();
	
	void setContinent(String value);
	void setLatitude(BigDecimal value);
	void setLongitude(BigDecimal value);
	void setReportId(Integer value);
	void setReportTime(Date value);
	void setTemperature(int value);
}
