package org.hivedb.util.database.test;

import org.hivedb.annotations.EntityId;
import org.hivedb.annotations.GeneratedClass;
import org.hivedb.annotations.PartitionIndex;
import org.hivedb.annotations.Resource;

@GeneratedClass("TemperatureGenerated")
@Resource("Temperature")
public interface Temperature {
	
	@PartitionIndex
	String getContinent();
	
	@EntityId
	Integer getTemperatureId();
}
