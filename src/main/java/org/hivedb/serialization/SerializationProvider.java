package org.hivedb.serialization;

import java.io.InputStream;
import java.util.Collection;


public interface SerializationProvider {
	public Serializer<Object, InputStream> getSerializer(Class<?> clazz);
	public Collection<Class> getSerializableInterfaces();
}
