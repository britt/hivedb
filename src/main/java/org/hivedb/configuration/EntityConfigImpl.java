package org.hivedb.configuration;

import java.io.Serializable;
import java.util.Collection;

import org.hivedb.hibernate.annotations.IndexType;
import org.hivedb.util.ReflectionTools;
import org.hivedb.util.functional.Filter;
import org.hivedb.util.functional.Predicate;

public class EntityConfigImpl implements EntityConfig {

	private Class<?> representedInterface;
	private String partitionDimensionName;
	private String resourceName;
	private String primaryIndexKeyPropertyName;
	private String idPropertyName;
	private Collection<? extends EntityIndexConfig> entityIndexConfigs;
	private boolean isPartitioningResource;
	
	public static EntityConfig createEntity(
			Class<?> representedInterface, 
			String partitionDimensionName,
			String resourceName,
			String primaryIndexPropertyName,
			String idPropertyName,
			Collection<? extends EntityIndexConfig> indexConfigs) {
		return new EntityConfigImpl(
				representedInterface, 
				partitionDimensionName,
				resourceName,
				primaryIndexPropertyName,
				idPropertyName,
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
			Collection<? extends EntityIndexConfig> secondaryIndexIdentifiables) {
		return new EntityConfigImpl(
				representedInterface, 
				partitionDimensionName,
				partitionDimensionName,
				idPropertyName,
				idPropertyName,
				secondaryIndexIdentifiables,
				true);
		
	}
	
	public EntityConfigImpl(
			Class<?> representedInterface,
			String partitionDimensionName,
			String resourceName,
			String primaryIndexKeyPropertyName,
			String idPropertyName,
			Collection<? extends EntityIndexConfig> entityIndexConfigs,
			boolean isPartitioningResource) {
		this.representedInterface = representedInterface;
		this.partitionDimensionName = partitionDimensionName;
		this.resourceName = resourceName;
		this.primaryIndexKeyPropertyName = primaryIndexKeyPropertyName;
		this.idPropertyName = idPropertyName;
		this.entityIndexConfigs = entityIndexConfigs;
		this.isPartitioningResource = isPartitioningResource;
	}
	
	public String getResourceName() {
		return resourceName;
	}
	
	public Collection<? extends EntityIndexConfig> getEntitySecondaryIndexConfigs() {
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
	public Collection<? extends EntityIndexConfig> getIndexConfigs(final IndexType indexType) {
		return Filter.grep(new Predicate<EntityIndexConfig>() {
			public boolean f(EntityIndexConfig entityIndexConfig) {
				return entityIndexConfig.getIndexType().equals(indexType);
			}},
			entityIndexConfigs);
	}
	
	public String getStoredVersionProperty() {
		return "version";
	}
}
