package org.hivedb.configuration;

import java.io.Serializable;
import java.util.Collection;

import org.hivedb.annotations.IndexType;
import org.hivedb.util.ReflectionTools;
import org.hivedb.util.functional.Filter;
import org.hivedb.util.functional.Predicate;

public class EntityConfigImpl implements EntityConfig {

	private Class<?> representedInterface;
	private String partitionDimensionName, resourceName, primaryIndexKeyPropertyName, idPropertyName, versionPropertyName;
	private Collection<? extends EntityIndexConfig> entityIndexConfigs;
	private boolean isPartitioningResource;
	
	public static EntityConfig createEntity(
			Class<?> representedInterface, 
			String partitionDimensionName,
			String resourceName,
			String primaryIndexPropertyName,
			String idPropertyName,
			String versionPropertyName,
			Collection<? extends EntityIndexConfig> indexConfigs) {
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
			Collection<? extends EntityIndexConfig> secondaryIndexIdentifiables) {
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
			Collection<? extends EntityIndexConfig> entityIndexConfigs,
			boolean isPartitioningResource) {
		this.representedInterface = representedInterface;
		this.partitionDimensionName = partitionDimensionName;
		this.resourceName = resourceName;
		this.primaryIndexKeyPropertyName = primaryIndexKeyPropertyName;
		this.idPropertyName = idPropertyName;
		this.entityIndexConfigs = entityIndexConfigs;
		this.isPartitioningResource = isPartitioningResource;
		this.versionPropertyName = versionPropertyName;
	}
	
	public String getResourceName() {
		return resourceName;
	}
	
	public Collection<? extends EntityIndexConfig> getEntityIndexConfigs() {
		return entityIndexConfigs;
	}
	
	public String getIdPropertyName() {
		return this.idPropertyName;
	}
	@SuppressWarnings("unchecked")
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
	public Collection<? extends EntityIndexConfig> getEntityIndexConfigs(final IndexType indexType) {
		return Filter.grep(new Predicate<EntityIndexConfig>() {
			public boolean f(EntityIndexConfig entityIndexConfig) {
				return entityIndexConfig.getIndexType().equals(indexType);
			}},
			entityIndexConfigs);
	}
	public EntityIndexConfig getEntityIndexConfig(final String propertyName) {
		return Filter.grepSingle(new Predicate<EntityIndexConfig>() {
			public boolean f(EntityIndexConfig entityIndexConfig) {
				return entityIndexConfig.getPropertyName().equals(propertyName);
			}},
			entityIndexConfigs);
	}
	
	public int getVersion(Object instance) {
		return versionPropertyName == null ? 
				0 : 
				((Integer) ReflectionTools.invokeGetter(instance, versionPropertyName));
	}
	
}
