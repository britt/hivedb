package org.hivedb.serialization;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import org.hibernate.shards.util.Lists;
import org.hivedb.util.ReflectionTools;
import org.hivedb.util.functional.Maps;
import org.hivedb.util.functional.Transform;
import org.hivedb.util.functional.Unary;


public class XmlXStreamSerializationProvider implements SerializationProvider {
	@SuppressWarnings("unchecked")
	Map<Class, XmlXStreamSerializer> serializers = Maps.newHashMap();
	
	private static XmlXStreamSerializationProvider instance = null;
	
	private XmlXStreamSerializationProvider() {}
	
	public static XmlXStreamSerializationProvider initialize(Class...classes){
		return initialize(Arrays.asList(classes));
	}
	
	public static XmlXStreamSerializationProvider initialize(Collection<Class> classes) {
		XmlXStreamSerializationProvider provider = new XmlXStreamSerializationProvider();
		
		provider.serializers = Transform.toMap(
				new Transform.IdentityFunction<Class>(),
				new Unary<Class, XmlXStreamSerializer>(){

					public XmlXStreamSerializer f(Class item) {
						return new XmlXStreamSerializer(item);
					}},
				classes);
		
		synchronized (provider) {
			XmlXStreamSerializationProvider.instance = provider;
		}
		return provider;
	}
 
	
	public static SerializationProvider instance() {
		if(instance == null) 
			throw new RuntimeException("SerializationProvider must be initialized.");
		return instance;
	}
	
	@SuppressWarnings("unchecked")
	public Serializer<Object, InputStream> getSerializer(Class<?> clazz) {
		return serializers.get(ReflectionTools.whichIsImplemented(clazz, serializers.keySet()));
	}


	public Map<Class, XmlXStreamSerializer> getSerializers() {
		return serializers;
	}

	public void setSerializers(Map<Class, XmlXStreamSerializer> serializers) {
		this.serializers = serializers;
	}

	public static XmlXStreamSerializationProvider getInstance() {
		return instance;
	}

	public static void setInstance(XmlXStreamSerializationProvider instance) {
		XmlXStreamSerializationProvider.instance = instance;
	}

	@SuppressWarnings("unchecked")
	public Collection<Class> getSerializableInterfaces() {
		return new ArrayList(serializers.keySet());
	}

}
