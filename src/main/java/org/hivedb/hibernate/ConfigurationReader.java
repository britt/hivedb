package org.hivedb.hibernate;

import org.hivedb.Hive;
import org.hivedb.HiveLockableException;
import org.hivedb.HiveRuntimeException;
import org.hivedb.annotations.*;
import org.hivedb.configuration.*;
import org.hivedb.management.HiveConfigurationSchemaInstaller;
import org.hivedb.meta.PartitionDimension;
import org.hivedb.meta.SecondaryIndex;
import org.hivedb.meta.ResourceImpl;
import org.hivedb.meta.persistence.CachingDataSourceProvider;
import org.hivedb.util.Lists;
import org.hivedb.util.PrimitiveUtils;
import org.hivedb.util.classgen.ReflectionTools;
import org.hivedb.util.database.JdbcTypeMapper;
import org.hivedb.util.functional.Atom;
import org.hivedb.util.functional.Filter;
import org.hivedb.util.functional.Predicate;
import org.springframework.beans.BeanUtils;

import java.lang.reflect.Method;
import java.util.*;

public class ConfigurationReader {
  private Map<String, EntityConfig> hiveConfigs = new HashMap<String, EntityConfig>();
  private PartitionDimension dimension = null;

  public ConfigurationReader(PartitionDimension partitionDimension) {
    dimension = partitionDimension;
  }

  public ConfigurationReader(Class<?>... classes) {
    for (Class<?> clazz : classes)
      if (isHiveEntity(clazz))
        configure(clazz);
  }

  public ConfigurationReader(Collection<Class<?>> classes) {
    for (Class<?> clazz : classes)
      if (isHiveEntity(clazz))
        configure(clazz);
  }

  private boolean isHiveEntity(Class<?> clazz) {
    return clazz.isAnnotationPresent(Resource.class);
  }

  public EntityConfig configure(Class<?> clazz) {
    EntityConfig config = readConfiguration(clazz);
    if (dimension == null)
      dimension = extractPartitionDimension(clazz);
    else if (!dimension.getName().equals(config.getPartitionDimensionName()))
      throw new UnsupportedOperationException(
        String.format("You are trying to configure on object from partition dimension %s into a Hive configured to use partition dimension %s. This is not supported. Use a separate configuration for each dimension.", config.getPartitionDimensionName(), dimension.getName()));

    hiveConfigs.put(clazz.getName(), config);
    return config;
  }

  @SuppressWarnings("unchecked")
  public static PartitionDimension extractPartitionDimension(Class clazz) {
    Method partitionIndexMethod = AnnotationHelper.getFirstMethodWithAnnotation(clazz, PartitionIndex.class);
    return new PartitionDimension(getPartitionDimensionName(clazz), JdbcTypeMapper.primitiveTypeToJdbcType(partitionIndexMethod.getReturnType()));
  }

  public static EntityConfig readConfiguration(Class<?> clazz) {
    PartitionDimension dimension = extractPartitionDimension(clazz);

    Method versionMethod = AnnotationHelper.getFirstMethodWithAnnotation(clazz, EntityVersion.class);
    Method resourceIdMethod = AnnotationHelper.getFirstMethodWithAnnotation(clazz, EntityId.class);
    Method partitionIndexMethod = AnnotationHelper.getFirstMethodWithAnnotation(clazz, PartitionIndex.class);

    String primaryIndexPropertyName = getIndexNameForMethod(partitionIndexMethod);
    String idPropertyName = getIndexNameForMethod(resourceIdMethod);
    String versionPropertyName = versionMethod == null ? null : getIndexNameForMethod(versionMethod);

    List<EntityIndexConfig> indexes = createIndexMethods(clazz, resourceIdMethod);

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

  private static List<EntityIndexConfig> createIndexMethods(Class<?> clazz, Method resourceIdMethod) {
    Collection<Method> indexMethods = getIndexMethods(clazz, resourceIdMethod);
    List<EntityIndexConfig> indexes = Lists.newArrayList();

    for (Method indexMethod : indexMethods)
      if (isCollectionPropertyOfAComplexType(clazz, indexMethod))
        if (isIndexDelegate(indexMethod))
          indexes.add(new EntityIndexConfigProxy(clazz, getSecondaryIndexName(indexMethod), getIndexPropertyOfCollectionType(ReflectionTools.getCollectionItemType(clazz, ReflectionTools.getPropertyNameOfAccessor(indexMethod))), readConfiguration(getHiveForeignKeyIndexClass(indexMethod))));
        else
          indexes.add(new EntityIndexConfigImpl(clazz, getSecondaryIndexName(indexMethod), getIndexPropertyOfCollectionType(ReflectionTools.getCollectionItemType(clazz, ReflectionTools.getPropertyNameOfAccessor(indexMethod)))));
      else if (isIndexDelegate(indexMethod))
        indexes.add(new EntityIndexConfigProxy(clazz, getSecondaryIndexName(indexMethod), readConfiguration(getHiveForeignKeyIndexClass(indexMethod))));
      else
        indexes.add(new EntityIndexConfigImpl(clazz, getSecondaryIndexName(indexMethod)));
    return indexes;
  }

  @SuppressWarnings("unchecked")
  private static Collection<Method> getIndexMethods(Class<?> clazz, final Method resourceIdMethod) {
    return Filter.grep(new Predicate<Method>() {
      public boolean f(Method method) {
        return !method.equals(resourceIdMethod);
      }
    }, AnnotationHelper.getAllMethodsWithAnnotations(clazz, (Collection) Arrays.asList(Index.class, PartitionIndex.class)));
  }

  private static boolean isIndexDelegate(Method indexMethod) {
    return indexMethod.getAnnotation(IndexDelegate.class) != null;
  }

  private static Class<?> getHiveForeignKeyIndexClass(Method indexMethod) {
    try {
      return Class.forName(indexMethod.getAnnotation(IndexDelegate.class).value());
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Class not found " + indexMethod.getAnnotation(IndexDelegate.class).value());
    }
  }

  private static boolean isCollectionPropertyOfAComplexType(Class<?> clazz, Method indexMethod) {
    return ReflectionTools.isCollectionProperty(clazz, ReflectionTools.getPropertyNameOfAccessor(indexMethod)) &&
      !PrimitiveUtils.isPrimitiveClass(ReflectionTools.getCollectionItemType(clazz, ReflectionTools.getPropertyNameOfAccessor(indexMethod)));
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
    return hiveConfigs.values();
  }

  public EntityConfig getEntityConfig(String className) {
    return hiveConfigs.get(className);
  }

  public EntityHiveConfig getHiveConfiguration() {
    EntityConfig prototype = Atom.getFirstOrThrow(hiveConfigs.values());
    return new PluralHiveConfig(hiveConfigs, prototype.getPartitionDimensionName(), prototype.getPrimaryKeyClass());
  }

  public void install(String uri) {
    new HiveConfigurationSchemaInstaller(uri).run();
    install(Hive.load(uri, CachingDataSourceProvider.getInstance()));
  }

  public void install(Hive hive) {
    Hive target = hive;
    if (hive.getPartitionDimension() == null) {
      target = Hive.create(hive.getUri(), dimension.getName(), dimension.getColumnType(), CachingDataSourceProvider.getInstance(), null);
    }

    for (EntityConfig config : hiveConfigs.values())
      installConfiguration(config, target);
  }

  public void installConfiguration(EntityConfig config, Hive hive) {
    try {
      // Duplicate installations are possible due to delegated indexes
      if (hive.doesResourceExist(config.getResourceName()))
        return;

      org.hivedb.meta.Resource resource =
        hive.addResource(createResource(config));

      for (EntityIndexConfig indexConfig : (Collection<EntityIndexConfig>) config.getEntityIndexConfigs())
        if (indexConfig.getIndexType().equals(IndexType.Delegates))
          installConfiguration(((EntityIndexConfigDelegator) indexConfig).getDelegateEntityConfig(), hive);
        else if (indexConfig.getIndexType().equals(IndexType.Hive)) {
          hive.addSecondaryIndex(resource, createSecondaryIndex(indexConfig));
        }

    } catch (HiveLockableException e) {
      throw new HiveRuntimeException(e.getMessage(), e);
    }
  }

  private SecondaryIndex createSecondaryIndex(EntityIndexConfig config) {
    return new SecondaryIndex(config.getIndexName(), JdbcTypeMapper.primitiveTypeToJdbcType(config.getIndexClass()));
  }

  private ResourceImpl createResource(EntityConfig config) {
    return new ResourceImpl(
      config.getResourceName(),
      JdbcTypeMapper.primitiveTypeToJdbcType(config.getIdClass()),
      config.isPartitioningResource());
  }

  public static String getResourceName(Class<?> clazz) {
    Resource resource = clazz.getAnnotation(Resource.class);
    if (resource != null)
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
    if (annotation != null && !"".equals(annotation.name()))
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
