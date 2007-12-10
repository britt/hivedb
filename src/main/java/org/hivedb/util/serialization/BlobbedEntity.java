package org.hivedb.util.serialization;

import org.hivedb.annotations.GeneratedClass;
import org.hivedb.annotations.HiveForeignKey;
import org.hivedb.util.database.test.WeatherReport;

@GeneratedClass(name="BlobbedEntityGenerated")
public interface BlobbedEntity {
	@HiveForeignKey(WeatherReport.class)
	Integer getId();
	void setId(Integer id);
	byte[] getValue();
	void setValue(byte[] value);
	WeatherReport getEntity();
	void setEntity(WeatherReport value);
}
