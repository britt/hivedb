package org.hivedb.util.database.test;

import java.io.Serializable;
import java.util.Collection;

import org.hivedb.annotations.EntityId;
import org.hivedb.annotations.GeneratedClass;
import org.hivedb.annotations.Index;


@GeneratedClass("WeatherEventGenerated")
public interface WeatherEvent extends Serializable {
	@Index
	@EntityId
	Integer getEventId();
	String getContinent();
	String getName();
	Collection<Integer> getStatistics();
	void setEventId(Integer value);
	void setContinent(String value);
	void setName(String name);
	void setStatistics(Collection<Integer> statistics);
}
