package org.hivedb.util;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;

import org.hibernate.shards.strategy.access.ShardAccessStrategy;
import org.hibernate.shards.util.Lists;
import org.hivedb.Hive;
import org.hivedb.HiveRuntimeException;
import org.hivedb.annotations.AnnotationHelper;
import org.hivedb.annotations.IndexParam;
import org.hivedb.configuration.EntityConfig;
import org.hivedb.configuration.EntityHiveConfig;
import org.hivedb.hibernate.BaseDataAccessObject;
import org.hivedb.hibernate.ConfigurationReader;
import org.hivedb.hibernate.DataAccessObject;
import org.hivedb.hibernate.EntityResolver;
import org.hivedb.hibernate.HiveSessionFactory;
import org.hivedb.hibernate.HiveSessionFactoryBuilderImpl;
import org.hivedb.services.Service;
import org.hivedb.services.ServiceContainer;
import org.hivedb.services.ServiceResponse;
import org.hivedb.util.GenerateInstance;
import org.hivedb.util.GeneratedClassFactory;
import org.hivedb.util.GeneratedInstanceInterceptor;
import org.hivedb.util.functional.Amass;
import org.hivedb.util.functional.Atom;
import org.hivedb.util.functional.Filter;
import org.hivedb.util.functional.Joiner;
import org.hivedb.util.functional.NumberIterator;
import org.hivedb.util.functional.Pair;
import org.hivedb.util.functional.Transform;
import org.hivedb.util.functional.Unary;

public class GeneratedServiceInterceptor implements MethodInterceptor, Service {

	public static Service load(Class clazz, Class serviceClass, Class serviceResponseClass, Class serviceContainerClass, Hive hive, EntityHiveConfig entityHiveConfig, ShardAccessStrategy strategy) {
		EntityConfig config = entityHiveConfig.getEntityConfig(clazz.getCanonicalName());
		List<Class<?>> classes = Lists.newArrayList();
		classes.addAll(new EntityResolver(entityHiveConfig).getEntityClasses());
		HiveSessionFactory factory = new HiveSessionFactoryBuilderImpl(hive.getUri(),classes,strategy);
		DataAccessObject dao = new BaseDataAccessObject(config, hive, factory);
		return (Service)GeneratedClassFactory.newInstance(serviceClass, new GeneratedServiceInterceptor(clazz, serviceClass, serviceResponseClass, serviceContainerClass, dao, config));
	}
	public static Service load(Class clazz, Class serviceClass, Class serviceResponseClass, Class serviceContainerClass, Hive hive, ConfigurationReader reader, ShardAccessStrategy strategy) {
		return load(clazz, serviceClass, serviceResponseClass, serviceContainerClass, hive, reader.getHiveConfiguration(), strategy);
	}
	
	Class clazz;
	Class serviceClass;
	Class serviceResponseClass;
	Class serviceContainerClass;
	DataAccessObject dao;
	EntityConfig config;
	public GeneratedServiceInterceptor(Class clazz, Class serviceClass, Class serviceResponseClass, Class serviceContainerClass, DataAccessObject dao, EntityConfig config) {
		this.clazz = clazz;
		this.serviceClass = serviceClass;
		this.serviceResponseClass = serviceResponseClass;
		this.serviceContainerClass = serviceContainerClass;
		this.dao = dao;
		this.config = config;
	}
	
	public Collection<Object> unProxy(Collection<Object> classInstances) {
		return Transform.map(new Unary<Object, Object>(){
			public Object f(Object item) {
				return unProxy(item);
			}}, classInstances);
	}
	
	public Object unProxy(Object instance) {
		return new GenerateInstance(clazz).generateAndCopyProperties(instance);
	}
	
	public boolean exists(Object id) {
		return dao.exists(id);
	}

	public String getPersistedClass() {
		return clazz.getName();
	}
	
	public Object delete(Object id) {
		dao.delete(id);
		return id;
	}	
	
	public ServiceResponse get(Object id) {
		final Object instance = dao.get(id);
		return formulateResponse((Collection<Object>)(instance != null ? Arrays.asList(instance) : Collections.emptyList()));
	}


	public ServiceResponse save(Object instance) {
		return formulateResponse(dao.save(unProxy(instance)));
	}

	public ServiceResponse saveAll(Iterable instances) {
		return formulateResponse(dao.saveAll((Collection) unProxy((Collection)instances)));
	}

	
	@SuppressWarnings("unchecked")
	private ServiceResponse formulateResponse(Object... instances) {
		return formulateResponse(Arrays.asList(instances));
	}
	@SuppressWarnings("unchecked")
	private ServiceResponse formulateResponse(Collection instances) {
		validateNonNull(instances);
		ServiceResponse serviceResponse = createServiceResponse(instances);
		return serviceResponse;
	}
	public ServiceResponse createServiceResponse(Collection instances) {
		ServiceResponse serviceResponse = (ServiceResponse) GeneratedClassFactory.newInstance(serviceResponseClass);
		GeneratedInstanceInterceptor.setProperty(serviceResponse, "containers", Transform.map(new Unary<Object, ServiceContainer>(){
			public ServiceContainer f(Object item) {
				return createServiceContainer(item, config.getVersion(item));
			}}, instances));
		return serviceResponse;
	}
	public ServiceContainer createServiceContainer(Object instance, Integer version) {
		ServiceContainer serviceContainer = (ServiceContainer) GeneratedClassFactory.newInstance(serviceContainerClass);
		GeneratedInstanceInterceptor.setProperty(serviceContainer, "instance", instance); 
		GeneratedInstanceInterceptor.setProperty(serviceContainer, "version", version);
		return serviceContainer;
	}
	
	
	private void validateNonNull(final Collection<Object> collection) {
		if (Filter.isMatch(new Filter.NullPredicate<Object>(), collection)) {
			String ids = Amass.joinByToString(new Joiner.ConcatStrings<String>(", "), 
				Transform.map(new Unary<Object, String>() {
					public String f(Object item) {
						return item != null ? config.getId(item).toString() : "null"; }}, collection));
			throw new HiveRuntimeException(String.format("Encountered null items in collection: %s", ids));
		}
	}

	public ServiceResponse findByProperty(String propertyName, Object value) {
		return formulateResponse(dao.findByProperty(propertyName, value));
	}

	public ServiceResponse findByPropertyRange(String propertyName, Object start, Object end) {
		return formulateResponse(dao.findByPropertyRange(propertyName, start, end));
	}

	public ServiceResponse findByProperties(String partitioningProperty, Map<String,Object> propertyNameValueMap){
		return formulateResponse(dao.findByProperties(partitioningProperty, propertyNameValueMap));
	}

	public Object intercept(Object obj, Method method, Object[] args, MethodProxy proxy) throws Throwable {
		Object retValFromSuper = null;
		try {
			if (!Modifier.isAbstract(method.getModifiers())) {
				retValFromSuper = proxy.invokeSuper(obj, args);
			}
		} 
		finally {}
		
		String name = method.getName();
		if( name.equals("get"))
			return get(args[0]);
		else if( name.equals("exists"))
			return exists(args[0]);
		else if (name.equals("save"))
			return save(args[0]);
		else if (name.equals("saveAll")) {
			return saveAll(Arrays.asList((Object[])args[0]));
		}
		else if (name.equals("delete"))
			return delete(args[0]);
		else if (name.equals("getPersistedClass"))
			return getPersistedClass();
		else if (method.getDeclaringClass().equals(serviceClass))
			return makeGeneralizedCall(obj, method, args);
		return retValFromSuper;
	}

	private Object makeGeneralizedCall(Object obj, final Method method, final Object[] args) {
		
		Collection<Entry<String,Object>> entries = Transform.map(new Unary<Integer, Entry<String,Object>>() {
				public Entry<String,Object> f(Integer index) {
					IndexParam annotation = AnnotationHelper.getMethodArgumentAnnotationDeeply(method, index-1, IndexParam.class);
					if (annotation == null)
						throw new RuntimeException(String.format("Parameter %s of method %s of class %s has no annoation", index-1, method.getName(), method.getDeclaringClass()));
					return new Pair<String, Object>(config.getEntityIndexConfig(((IndexParam)annotation).value()).getPropertyName(), args[index-1]);
				}
			}, new NumberIterator(args.length));
		
		if (entries.size()==1) {
			Entry<String, Object> entry = Atom.getFirstOrThrow(entries);
			return findByProperty(entry.getKey(), entry.getValue());
		}
		if (entries.size()==2) {
			Entry<String, Object> entry1 = Atom.getFirstOrThrow(entries);
			Entry<String, Object> entry2 = Atom.getFirstOrThrow(Atom.getRestOrThrow(entries));
			if (entry1.getKey().equals(entry2.getKey()))
				return findByPropertyRange(entry1.getKey(), entry1.getValue(), entry2.getValue());
		}
		// TODO assumes the first parameter is the partitioning parameter. Use the anotation to specify
		return findByProperties(Atom.getFirstOrThrow(entries).getKey(), Transform.toOrderedMap(entries));
	}
}
