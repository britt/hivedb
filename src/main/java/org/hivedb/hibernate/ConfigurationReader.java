package org.hivedb.hibernate;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.hivedb.Hive;
import org.hivedb.HiveReadOnlyException;
import org.hivedb.HiveRuntimeException;
import org.hivedb.configuration.EntityConfig;
import org.hivedb.configuration.EntityConfigImpl;
import org.hivedb.configuration.EntityHiveConfig;
import org.hivedb.configuration.EntityIndexConfig;
import org.hivedb.configuration.EntityIndexConfigImpl;
import org.hivedb.configuration.PluralHiveConfig;
import org.hivedb.hibernate.annotations.AnnotationHelper;
import org.hivedb.hibernate.annotations.EntityId;
import org.hivedb.hibernate.annotations.Index;
import org.hivedb.hibernate.annotations.PartitionIndex;
import org.hivedb.hibernate.annotations.Resource;
import org.hivedb.meta.Node;
import org.hivedb.meta.SecondaryIndex;
import org.hivedb.util.Lists;
import org.hivedb.util.database.JdbcTypeMapper;
import org.hivedb.util.functional.Transform;
import org.hivedb.util.functional.Unary;
import org.springframework.beans.BeanUtils;

public class ConfigurationReader {
	@SuppressWarnings("unchecked")
	private Map<String, EntityConfig> configs = new HashMap<String, EntityConfig>();
	
	public ConfigurationReader() {}
	
	public ConfigurationReader(Class<?>... classes) {
		for(Class<?> clazz : classes)
			configure(clazz);
	}
	
	public ConfigurationReader(Collection<Class<?>> classes) {
		for(Class<?> clazz : classes)
			configure(clazz);
	}
	
	@SuppressWarnings("unchecked")
	public EntityConfig configure(Class<?> clazz) {
		String dimensionName = getPartitionDimensionName(clazz);
		Method partitionIndexMethod = AnnotationHelper.getFirstMethodWithAnnotation(clazz, PartitionIndex.class);
		Method resourceIdMethod = AnnotationHelper.getFirstMethodWithAnnotation(clazz, EntityId.class);
		List<Method> indexMethods = AnnotationHelper.getAllMethodsWithAnnotation(clazz, Index.class);
		if(indexMethods.contains(resourceIdMethod))
			indexMethods.remove(resourceIdMethod);
	
		String primaryIndexPropertyName = getIndexNameForMethod(partitionIndexMethod);
		String idPropertyName = getIndexNameForMethod(resourceIdMethod);
		
		List<EntityIndexConfig> indexes = Lists.newArrayList();
		
		for(Method indexMethod : indexMethods)
			indexes.add(new EntityIndexConfigImpl(clazz, getIndexNameForMethod(indexMethod)));
		
		EntityConfig config = new EntityConfigImpl(
				clazz,
				dimensionName,
				getResourceName(clazz),
				primaryIndexPropertyName,
				idPropertyName,
				indexes,
				partitionIndexMethod.getName().equals(resourceIdMethod.getName())
		);
	
		configs.put(clazz.getName(), config);
		return config;
	}
	
	@SuppressWarnings("unchecked")
	public Collection<EntityConfig> getConfigurations() {
		 return configs.values();
	}

	public EntityHiveConfig getHiveConfiguration(Hive hive) {
		return new PluralHiveConfig(configs, hive);
	}
	
	@SuppressWarnings("unchecked")
	public void install(Hive hive) {
		for(EntityConfig config : configs.values())
			installConfiguration(config,hive);
	}
	
	@SuppressWarnings("unchecked")
	public void installConfiguration(EntityConfig config, Hive hive) {
		try {
			org.hivedb.meta.Resource resource = 
				hive.addResource(createResource(config));
					
			for(EntityIndexConfig indexConfig : (Collection<EntityIndexConfig>) config.getEntitySecondaryIndexConfigs())
				hive.addSecondaryIndex(resource, createSecondaryIndex(indexConfig));
		} catch (HiveReadOnlyException e) {
			throw new HiveRuntimeException(e.getMessage(),e);
		}
	}

	private SecondaryIndex createSecondaryIndex(EntityIndexConfig config) {
		return new SecondaryIndex(config.getIndexName(), JdbcTypeMapper.primitiveTypeToJdbcType(config.getIndexClass()));
	}
	
	@SuppressWarnings("unchecked")
	private org.hivedb.meta.Resource createResource(EntityConfig config) {
		return new org.hivedb.meta.Resource(
				config.getResourceName(), 
				JdbcTypeMapper.primitiveTypeToJdbcType(config.getIdClass()),
				config.isPartitioningResource());
	}
	
	public String getResourceName(Class<?> clazz) {
		Resource resource = clazz.getAnnotation(Resource.class);
		if(resource != null)
			return resource.name();
		else
			return getResourceNameForClass(clazz);
	}

	private String getPartitionDimensionName(Class<?> clazz) {
		return AnnotationHelper.getFirstInstanceOfAnnotation(clazz, PartitionIndex.class).name();
	}
	
	public static String getIndexNameForMethod(Method method) {
		return BeanUtils.findPropertyForMethod(method).getDisplayName();
	}

	public static String getResourceNameForClass(Class<?> clazz) {
		return clazz.getName().replace('.', '_');
	}
}
