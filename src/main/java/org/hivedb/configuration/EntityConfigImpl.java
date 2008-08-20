package org.hivedb.configuration;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.EnumSet;

import org.hivedb.annotations.AnnotationHelper;
import org.hivedb.annotations.Index;
import org.hivedb.annotations.IndexType;
import org.hivedb.util.classgen.ReflectionTools;
import org.hivedb.util.functional.Filter;
import org.hivedb.util.functional.Predicate;
import org.hivedb.util.functional.Transform;
import org.hivedb.util.functional.Unary;
import org.hivedb.util.functional.Filter.BinaryPredicate;

public class EntityConfigImpl implements EntityConfig {

	private Class<?> representedInterface;
	private String partitionDimensionName, resourceName, primaryIndexKeyPropertyName, idPropertyName, versionPropertyName;
	private Collection<EntityIndexConfig> entityIndexConfigs;
	private boolean isPartitioningResource;
	private Collection<Class<?>> associatedClasses;
	
	public static EntityConfig createEntity(
			Class<?> representedInterface, 
			String partitionDimensionName,
			String resourceName,
			String primaryIndexPropertyName,
			String idPropertyName,
			String versionPropertyName,
			Collection<EntityIndexConfig> indexConfigs) {
		return new EntityConfigImpl(
				representedInterface, 
				partitionDimensionName,
				resourceName,
				primaryIndexPropertyName,
				idPropertyName,
				versionPropertyName,
				indexConfigs,
				false);
	}
	/***
	 *  Create a configuration for an entity where the resource and partition dimension have the same index
	 * @param representedInterface
	 * @param partitionDimensionName
	 * @param resourceName
	 * @param secondaryIndexIdentifiables
	 * @param isPartitioningResource
	 * @param idPropertyName
	 * @return
	 */
	public static EntityConfig createPartitioningResourceEntity(
			Class<?> representedInterface, 
			String partitionDimensionName,
			String idPropertyName,
			String versionPropertyName,
			Collection<EntityIndexConfig> secondaryIndexIdentifiables) {
		return new EntityConfigImpl(
				representedInterface, 
				partitionDimensionName,
				partitionDimensionName,
				idPropertyName,
				idPropertyName,
				versionPropertyName,
				secondaryIndexIdentifiables,
				true);
		
	}
	
	public EntityConfigImpl(
			Class<?> representedInterface,
			String partitionDimensionName,
			String resourceName,
			String primaryIndexKeyPropertyName,
			String idPropertyName,
			String versionPropertyName,
			Collection<EntityIndexConfig> entityIndexConfigs,
			boolean isPartitioningResource) {
		this.representedInterface = representedInterface;
		this.partitionDimensionName = partitionDimensionName;
		this.resourceName = resourceName;
		this.primaryIndexKeyPropertyName = primaryIndexKeyPropertyName;
		this.idPropertyName = idPropertyName;
		this.entityIndexConfigs = entityIndexConfigs;
		this.isPartitioningResource = isPartitioningResource;
		this.versionPropertyName = versionPropertyName;
		this.associatedClasses = findAssociatedClasses(representedInterface);
	}
	
	private Collection<Class<?>> findAssociatedClasses(final Class<?> representedInterface) {
		// Search for indexed properties with complex class types of collections therein of.
		// Ingore properties that delegate, since such classes are not mapped.
		return Transform.map(new Unary<Method, Class<?>>() {
			public Class<?> f(Method method) {
				String property = ReflectionTools.getPropertyNameOfAccessor(method);
				return ReflectionTools.isCollectionProperty(representedInterface, property)
					? ReflectionTools.getCollectionItemType(representedInterface, property)
					: method.getReturnType();
			}},
			Filter.grep(new Predicate<Method>() {
				public boolean f(Method method) {
					String property = ReflectionTools.getPropertyNameOfAccessor(method);
					if (ReflectionTools.isCollectionProperty(representedInterface, property)
						&& !ReflectionTools.isComplexCollectionItemProperty(representedInterface, property))
						return false;
					Index index = AnnotationHelper.getAnnotationDeeply(method, Index.class);
					return index != null && index.type() != IndexType.Delegates;
			}}, ReflectionTools.getComplexGetters(representedInterface)));
		
	}
	
	public String getResourceName() {
		return resourceName;
	}
	
	public Collection<EntityIndexConfig> getEntityIndexConfigs() {
		return entityIndexConfigs;
	}
	
	public String getIdPropertyName() {
		return this.idPropertyName;
	}
	
	public Serializable getId(Object instance) {
		return (Serializable) ReflectionTools.invokeGetter(instance, idPropertyName);
	}

	public boolean isPartitioningResource() {
		return isPartitioningResource;
	}

	public Class<?> getRepresentedInterface() {
		return representedInterface;
	}

	public String getPartitionDimensionName() {
		return partitionDimensionName;
	}

	public String getPrimaryIndexKeyPropertyName() {
		return primaryIndexKeyPropertyName;
	}
	
	public Object getPrimaryIndexKey(Object resourceInstance) {
		return ReflectionTools.invokeGetter(resourceInstance, primaryIndexKeyPropertyName);
	}
	
	public Class<?> getPrimaryKeyClass() {
		return ReflectionTools.getPropertyType(getRepresentedInterface(), getPrimaryIndexKeyPropertyName());
	}
	
	public Class<?> getIdClass() {
		return ReflectionTools.getPropertyType(getRepresentedInterface(), getIdPropertyName());
	}
	public Collection<EntityIndexConfig> getEntityIndexConfigs(EnumSet<IndexType> indexTypes) {
		return Filter.grepAgainstList(
				indexTypes, entityIndexConfigs,new BinaryPredicate<IndexType, EntityIndexConfig>() {
					public boolean f(IndexType indexType, EntityIndexConfig entityIndexConfig) {
						return entityIndexConfig.getIndexType().equals(indexType);
					}});
	}
	public Collection<EntityIndexConfig> getEntityIndexConfigs(final IndexType indexType) {
		return getEntityIndexConfigs(EnumSet.of(indexType));
	}
	public EntityIndexConfig getEntityIndexConfig(final String propertyName) {
		return Filter.grepSingle(new Predicate<EntityIndexConfig>() {
			public boolean f(EntityIndexConfig entityIndexConfig) {
				return entityIndexConfig.getPropertyName().equals(propertyName);
			}},
			entityIndexConfigs);
	}
	
	public int getVersion(Object instance) {
		Integer version = null;
		if(versionPropertyName != null)
			version = ((Integer) ReflectionTools.invokeGetter(instance, versionPropertyName));
		return version == null ? 0 : version;
		
	}
	
	public EntityIndexConfig getPrimaryIndexKeyEntityIndexConfig() {
		return getEntityIndexConfig(getPrimaryIndexKeyPropertyName());
	}
	
	public Collection<Class<?>> getAssociatedClasses() {
		return associatedClasses;
	}
}
