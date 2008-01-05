package org.hivedb.hibernate;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.hivedb.Hive;
import org.hivedb.HiveFacade;
import org.hivedb.HiveReadOnlyException;
import org.hivedb.HiveRuntimeException;
import org.hivedb.annotations.AnnotationHelper;
import org.hivedb.annotations.EntityId;
import org.hivedb.annotations.EntityVersion;
import org.hivedb.annotations.HiveForeignKey;
import org.hivedb.annotations.Index;
import org.hivedb.annotations.IndexType;
import org.hivedb.annotations.PartitionIndex;
import org.hivedb.annotations.Resource;
import org.hivedb.configuration.EntityConfig;
import org.hivedb.configuration.EntityConfigImpl;
import org.hivedb.configuration.EntityHiveConfig;
import org.hivedb.configuration.EntityIndexConfig;
import org.hivedb.configuration.EntityIndexConfigImpl;
import org.hivedb.configuration.EntityIndexConfigProxy;
import org.hivedb.configuration.PluralHiveConfig;
import org.hivedb.management.HiveInstaller;
import org.hivedb.meta.PartitionDimension;
import org.hivedb.meta.SecondaryIndex;
import org.hivedb.util.Lists;
import org.hivedb.util.PrimitiveUtils;
import org.hivedb.util.ReflectionTools;
import org.hivedb.util.database.JdbcTypeMapper;
import org.hivedb.util.functional.Atom;
import org.springframework.beans.BeanUtils;

public class ConfigurationReader {
	private Map<String, EntityConfig> configs = new HashMap<String, EntityConfig>();
	private PartitionDimension dimension = null;
	
	public ConfigurationReader() {}
	
	public ConfigurationReader(Class<?>... classes) {
		for(Class<?> clazz : classes)
			if (isHiveEntity(clazz))
				configure(clazz);
	}
	
	public ConfigurationReader(Collection<Class<?>> classes) {
		for(Class<?> clazz : classes)
			if (isHiveEntity(clazz))
				configure(clazz);
	}
	
	private boolean isHiveEntity(Class<?> clazz) {
		return clazz.isAnnotationPresent(Resource.class);
	}

	public EntityConfig configure(Class<?> clazz) {
		EntityConfig config = readConfiguration(clazz) ;
		if(dimension == null) 
			dimension = new PartitionDimension(config.getPartitionDimensionName(), JdbcTypeMapper.primitiveTypeToJdbcType(config.getPrimaryKeyClass()));
		else
			if(!dimension.getName().equals(config.getPartitionDimensionName()))
				throw new UnsupportedOperationException(
						String.format("You are trying to configure on object from partition dimension %s into a Hive configured to use partition dimension %s. THis is not supported. Use a separate configuration for each dimension.", config.getPartitionDimensionName(), dimension.getName()));
		
		configs.put(clazz.getName(), config);
		return config;
	}
	
	public static EntityConfig readConfiguration(Class<?> clazz) {
		Method partitionIndexMethod = AnnotationHelper.getFirstMethodWithAnnotation(clazz, PartitionIndex.class);
		
		PartitionDimension dimension = new PartitionDimension(getPartitionDimensionName(clazz), JdbcTypeMapper.primitiveTypeToJdbcType(partitionIndexMethod.getReturnType()));
		
		Method versionMethod = AnnotationHelper.getFirstMethodWithAnnotation(clazz, EntityVersion.class);
		Method resourceIdMethod = AnnotationHelper.getFirstMethodWithAnnotation(clazz, EntityId.class);

		List<Method> indexMethods = getHiveIndexMethods(clazz,resourceIdMethod);	
		String primaryIndexPropertyName = getIndexNameForMethod(partitionIndexMethod);
		String idPropertyName = getIndexNameForMethod(resourceIdMethod);
		String versionPropertyName = versionMethod == null ? null : getIndexNameForMethod(versionMethod);
		
		List<EntityIndexConfig> indexes = Lists.newArrayList();
		
		for(Method indexMethod : indexMethods)
			if (isCollectionPropertyOfAComplexType(clazz, indexMethod))
				indexes.add(new EntityIndexConfigImpl(clazz, getSecondaryIndexName(indexMethod), getIndexPropertyOfCollectionType(ReflectionTools.getCollectionItemType(clazz, ReflectionTools.getPropertyNameOfAccessor(indexMethod)))));
			else
				if (isHiveForeignKeyIndex(indexMethod))
					indexes.add(new EntityIndexConfigProxy(clazz, getSecondaryIndexName(indexMethod), readConfiguration(getHiveForeignKeyIndexClass(indexMethod))));
				else
					indexes.add(new EntityIndexConfigImpl(clazz, getSecondaryIndexName(indexMethod)));
		
		EntityConfig config = new EntityConfigImpl(
				clazz,
				dimension.getName(),
				getResourceName(clazz),
				primaryIndexPropertyName,
				idPropertyName,
				versionPropertyName,
				indexes,
				partitionIndexMethod.getName().equals(resourceIdMethod.getName())
		);	
		return config;
	}
	
	private static boolean isHiveForeignKeyIndex(Method indexMethod) {
		return indexMethod.getAnnotation(HiveForeignKey.class) != null;
	}
	private static Class<?> getHiveForeignKeyIndexClass(Method indexMethod) {
		return indexMethod.getAnnotation(HiveForeignKey.class).value();
	}

	
	private static List<Method> getHiveIndexMethods(Class<?> clazz, Method resourceIdMethod) {
		List<Method> indexMethods = AnnotationHelper.getAllMethodsWithAnnotation(clazz, Index.class);
		if(indexMethods.contains(resourceIdMethod))
			indexMethods.remove(resourceIdMethod);
		
		List<Method> dataIndexes = Lists.newArrayList();
		for(Method indexMethod : indexMethods) {
			Index annotation = indexMethod.getAnnotation(Index.class);
			if(annotation.type() == IndexType.Data)
				dataIndexes.add(indexMethod);
		}
		indexMethods.removeAll(dataIndexes);
		return indexMethods;
	}

	private static boolean isCollectionPropertyOfAComplexType(Class<?> clazz, Method indexMethod) {
		return ReflectionTools.isCollectionProperty(clazz, ReflectionTools.getPropertyNameOfAccessor(indexMethod)) &&
			!PrimitiveUtils.isPrimitiveClass(ReflectionTools.getCollectionItemType(clazz,ReflectionTools.getPropertyNameOfAccessor(indexMethod)));
	}

	@SuppressWarnings("unchecked")
	private static String getIndexPropertyOfCollectionType(Class collectionType) {
		try {
			return ReflectionTools.getPropertyNameOfAccessor(AnnotationHelper.getFirstMethodWithAnnotation(collectionType, Index.class));
		}
		catch (Exception e) {
			throw new RuntimeException(String.format("Unable to find an Index annotation for collection type %s", collectionType.getName()));
		}
	}
	
	public Collection<EntityConfig> getConfigurations() {
		 return configs.values();
	}
	public EntityConfig getEntityConfig(String className) {
		return configs.get(className);
	}

	public EntityHiveConfig getHiveConfiguration() {
		EntityConfig prototype = Atom.getFirstOrThrow(configs.values());
		return new PluralHiveConfig(configs, prototype.getPartitionDimensionName(), prototype.getPrimaryKeyClass());
	}
	
	public void install(String uri) {
		new HiveInstaller(uri).run();
		install(Hive.load(uri));
	}
	
	public void install(HiveFacade hive) {
		HiveFacade target = hive;
		if(hive.getPartitionDimension() == null) {
			target = Hive.create(hive.getUri(), dimension.getName(), dimension.getColumnType());
		}
		
		for(EntityConfig config : configs.values())
			installConfiguration(config,target);
	}
	
	@SuppressWarnings("unchecked")
	public void installConfiguration(EntityConfig config, HiveFacade hive) {
		try {
			org.hivedb.meta.Resource resource = 
				hive.addResource(createResource(config));
					
			for(EntityIndexConfig indexConfig : (Collection<EntityIndexConfig>) config.getEntitySecondaryIndexConfigs())
				if (!indexConfig.getIndexType().equals(IndexType.HiveForeignKey))
					hive.addSecondaryIndex(resource, createSecondaryIndex(indexConfig));
		} catch (HiveReadOnlyException e) {
			throw new HiveRuntimeException(e.getMessage(),e);
		}
	}

	private SecondaryIndex createSecondaryIndex(EntityIndexConfig config) {
		return new SecondaryIndex(config.getIndexName(), JdbcTypeMapper.primitiveTypeToJdbcType(config.getIndexClass()));
	}
	
	private org.hivedb.meta.Resource createResource(EntityConfig config) {
		return new org.hivedb.meta.Resource(
				config.getResourceName(), 
				JdbcTypeMapper.primitiveTypeToJdbcType(config.getIdClass()),
				config.isPartitioningResource());
	}
	
	public static String getResourceName(Class<?> clazz) {
		Resource resource = clazz.getAnnotation(Resource.class);
		if(resource != null)
			return resource.value();
		else
			return getResourceNameForClass(clazz);
	}

	private static String getPartitionDimensionName(Class<?> clazz) {
		String name = AnnotationHelper.getFirstInstanceOfAnnotation(clazz, PartitionIndex.class).value();
		Method m = AnnotationHelper.getFirstMethodWithAnnotation(clazz, PartitionIndex.class);
		return "".equals(name) ? getIndexNameForMethod(m) : name;
	}
	
	private static String getSecondaryIndexName(Method method) {
		Index annotation = method.getAnnotation(Index.class);
		if(annotation != null && !"".equals(annotation.name()))
			return annotation.name();
		else
			return getIndexNameForMethod(method);
	}
	
	public static String getIndexNameForMethod(Method method) {
		return BeanUtils.findPropertyForMethod(method).getDisplayName();
	}

	public static String getResourceNameForClass(Class<?> clazz) {
		return clazz.getName().replace('.', '_');
	}
}
