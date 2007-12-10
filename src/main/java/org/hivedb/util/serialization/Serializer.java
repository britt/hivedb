package org.hivedb.util.serialization;

public interface Serializer<RAW,SERIAL> {
	SERIAL serialize(RAW raw);
	RAW deserialize(SERIAL serial);
	Class<RAW> getRepresentedInterface();
}
