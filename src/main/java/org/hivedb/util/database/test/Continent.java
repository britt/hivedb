package org.hivedb.util.database.test;

import org.hivedb.annotations.EntityId;
import org.hivedb.annotations.Index;
import org.hivedb.annotations.PartitionIndex;
import org.hivedb.annotations.Resource;

@Resource(name=WeatherReport.CONTINENT)
public interface Continent {
	@EntityId
	@PartitionIndex(name=WeatherReport.CONTINENT)
	public String getName();
	public void setName(String name);
	@Index
	public Integer getPopulation();
	public void setPopulation(Integer population);
}
