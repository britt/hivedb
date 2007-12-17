package org.hivedb.services;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;

import javax.jws.WebMethod;
import javax.jws.WebService;

import org.hibernate.shards.strategy.access.SequentialShardAccessStrategy;
import org.hibernate.shards.util.Lists;
import org.hibernate.shards.util.Maps;
import org.hivedb.Hive;
import org.hivedb.annotations.AnnotationHelper;
import org.hivedb.annotations.Resource;
import org.hivedb.configuration.EntityConfig;
import org.hivedb.hibernate.BaseDataAccessObject;
import org.hivedb.hibernate.ConfigurationReader;
import org.hivedb.hibernate.DataAccessObject;
import org.hivedb.hibernate.HiveSessionFactory;
import org.hivedb.hibernate.HiveSessionFactoryBuilderImpl;
import org.hivedb.util.GenerateInstance;
import org.hivedb.util.GeneratePrimitiveValue;
import org.hivedb.util.PrimitiveUtils;
import org.hivedb.util.ReflectionTools;
import org.hivedb.util.functional.Generator;
import org.hivedb.util.functional.Transform;
import org.hivedb.util.functional.Unary;

@WebService
public class DataGenerationService {
	private ConfigurationReader config;
	private Map<Class, DataAccessObject<Object, Serializable>> daos = Maps.newHashMap();
	
	public DataGenerationService(Collection<Class<?>> classes, Hive hive) {
		config = new ConfigurationReader(classes);
		HiveSessionFactory factory = new HiveSessionFactoryBuilderImpl(config.getHiveConfiguration(), classes, hive, new SequentialShardAccessStrategy());
		for(Class clazz : classes)
			if(clazz.getAnnotation(Resource.class) != null)
				daos.put(clazz, new BaseDataAccessObject(config.getEntityConfig(clazz.getName()),hive,factory));
	}
	
	public Collection<String> listClasses() {
		return Transform.map(new Unary<EntityConfig, String>(){
			public String f(EntityConfig item) {
				return item.getRepresentedInterface().getName();
			}}, config.getConfigurations());
	}
	
	@WebMethod
	public Collection<Serializable> generate(String clazz, int partitionKeyCount, int instanceCount) {
		Collection<Serializable> ids = Lists.newArrayList();
		EntityConfig entityConfig = config.getEntityConfig(clazz);
		Generator<Object> pKeyGenerator;
		if(PrimitiveUtils.isPrimitiveClass(entityConfig.getPrimaryKeyClass()))
			pKeyGenerator = new GeneratePrimitiveValue<Object>((Class<Object>) entityConfig.getPrimaryKeyClass());
		else
			pKeyGenerator = new GenerateInstance<Object>((Class<Object>) entityConfig.getPrimaryKeyClass());
		for(int i=0; i<partitionKeyCount; i++) {
			Object pkey = pKeyGenerator.generate();
			DataAccessObject<Object, Serializable> dao = daos.get(entityConfig.getRepresentedInterface());
			GenerateInstance<Object> instanceGenerator = new GenerateInstance<Object>((Class<Object>) entityConfig.getRepresentedInterface());
			for(int j=0; j<instanceCount; j++){
				Object instance = instanceGenerator.generate();
				ReflectionTools.invokeSetter(instance, entityConfig.getPrimaryIndexKeyPropertyName(), pkey);
				dao.save(instance);
				ids.add(entityConfig.getId(instance));
			}
		}
		return ids;
	}
}
