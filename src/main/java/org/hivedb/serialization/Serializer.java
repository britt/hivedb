package org.hivedb.serialization;

public interface Serializer<RAW,SERIAL> {
	SERIAL serialize(RAW raw);
	RAW deserialize(SERIAL serial);
	Class<RAW> getRepresentedInterface();
}
