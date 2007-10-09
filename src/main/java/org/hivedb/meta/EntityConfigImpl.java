package org.hivedb.meta;

import java.util.Collection;

import org.hivedb.util.ReflectionTools;

public class EntityConfigImpl<F> implements EntityConfig<F> {

	private Class<?> representedInterface;
	private String partitionDimensionName;
	private String resourceName;
	private String primaryIndexKeyPropertyName;
	private String idPropertyName;
	private Collection<? extends EntityIndexConfig> secondaryIndexIdentifiables;
	private boolean isPartitioningResource;
	
	public static EntityConfig<Object> createEntity(
			Class<?> representedInterface, 
			String partitionDimensionName,
			String resourceName,
			String primaryIndexPropertyName,
			String idPropertyName,
			Collection<? extends EntityIndexConfig> secondaryIndexIdentifiables) {
		return new EntityConfigImpl<Object>(
				representedInterface, 
				partitionDimensionName,
				resourceName,
				primaryIndexPropertyName,
				idPropertyName,
				secondaryIndexIdentifiables,
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
	public static EntityConfig<Object> createPartitioningResourceEntity(
			Class<?> representedInterface, 
			String partitionDimensionName,
			Collection<? extends EntityIndexConfig> secondaryIndexIdentifiables,
			String idPropertyName) {
		return new EntityConfigImpl<Object>(
				representedInterface, 
				partitionDimensionName,
				partitionDimensionName,
				idPropertyName,
				idPropertyName,
				secondaryIndexIdentifiables,
				true);
		
	}
	
	private EntityConfigImpl(
			Class<?> representedInterface,
			String partitionDimensionName,
			String resourceName,
			String primaryIndexKeyPropertyName,
			String idPropertyName,
			Collection<? extends EntityIndexConfig> secondaryIndexIdentifiables,
			boolean isPartitioningResource) {
		this.representedInterface = representedInterface;
		this.partitionDimensionName = partitionDimensionName;
		this.resourceName = resourceName;
		this.primaryIndexKeyPropertyName = primaryIndexKeyPropertyName;
		this.idPropertyName = idPropertyName;
		this.secondaryIndexIdentifiables = secondaryIndexIdentifiables;
		this.isPartitioningResource = isPartitioningResource;
	}
	
	public String getResourceName() {
		return resourceName;
	}
	
	public Collection<? extends EntityIndexConfig> getEntitySecondaryIndexConfigs() {
		return secondaryIndexIdentifiables;
	}
	
	public String getIdPropertyName() {
		return this.idPropertyName;
	}
	@SuppressWarnings("unchecked")
	public F getId(Object instance) {
		return (F)ReflectionTools.invokeGetter(instance, idPropertyName);
	}

	public boolean isPartitioningResource() {
		return isPartitioningResource;
	}

	public Class getRepresentedInterface() {
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
}
