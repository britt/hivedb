package org.hivedb.util.database.test;

import java.util.Collection;

import org.hivedb.annotations.EntityId;
import org.hivedb.annotations.GeneratedClass;
import org.hivedb.annotations.Index;


@GeneratedClass("WeatherEventGenerated")
public interface WeatherEvent {
	@Index
	Integer getEventId();
	String getContinent();
	String getName();
	Collection<Integer> getStatistics();
	void setEventId(Integer value);
	void setContinent(String value);
	void setName(String name);
	void setStatistics(Collection<Integer> statistics);
}
