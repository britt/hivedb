package org.hivedb.hibernate;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;

import org.hibernate.Interceptor;
import org.hivedb.configuration.EntityHiveConfig;
import org.hivedb.util.GenerateInstance;
import org.hivedb.util.GeneratedInstanceInterceptor;
import org.hivedb.util.PropertySetter;
import org.hivedb.util.functional.Transform;
import org.hivedb.util.functional.Unary;

public class DynamicMapDataAccessObject extends BaseDataAccessObject {
	private GenerateInstance<Object> generator;
	
	@SuppressWarnings("unchecked")
	public DynamicMapDataAccessObject(Class<?> clazz, EntityHiveConfig config,HiveSessionFactory factory) {
		super(clazz, config, factory);
		this.generator = new GenerateInstance<Object>((Class<Object>) clazz);
	}

	@SuppressWarnings("unchecked")
	public DynamicMapDataAccessObject(Class<?> clazz, EntityHiveConfig config, HiveSessionFactory factory, Interceptor interceptor) {
		super(clazz, config, factory, interceptor);
		this.generator = new GenerateInstance<Object>((Class<Object>) clazz);
	}
	
	@Override
	public Collection<Object> findByProperty(String propertyName, Object value) {
		return instantiateCollection(super.findByProperty(propertyName, value));
	}

	@Override
	public Collection<Object> findByPropertyRange(String propertyName, Object minValue, Object maxValue) {
		return instantiateCollection(super.findByPropertyRange(propertyName, minValue, maxValue));
	}

	@Override
	public Object get(Serializable id) {
		return instantiator().f(super.get(id));
	}

	@Override
	public Object save(Object entity) {	
		return instantiator().f(super.save(convertToMap(entity)));
	}

	@Override
	public Collection<Object> saveAll(Collection<Object> collection) {
		return instantiateCollection(super.saveAll(convertCollectionToMaps(collection)));
	}
	
	private Collection<Object> instantiateCollection(Collection<?> items) {
		return Transform.map(instantiator(), items);	
	}

	private Unary<Object, Object> instantiator() {
		return new Unary<Object, Object>(){
			@SuppressWarnings("unchecked")
			public Object f(Object item) {
				return GeneratedInstanceInterceptor.newInstance(getRespresentedClass(), (Map<String,Object>)item);
			}};
	}


	private Object convertToMap(Object entity) {
		generator.generateAndCopyProperties(entity);
		return ((PropertySetter<?>)generator.generateAndCopyProperties(entity)).getAsMap();
	}


	private Collection<Object> convertCollectionToMaps(Collection<Object> collection) {
		return Transform.map(new Unary<Object, Object>(){
			public Object f(Object item) {
				return convertToMap(item);
			}}, collection);
	}
}
