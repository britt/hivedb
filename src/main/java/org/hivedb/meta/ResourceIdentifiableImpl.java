package org.hivedb.meta;

import java.util.Collection;

import org.hivedb.util.ReflectionTools;

public class ResourceIdentifiableImpl<F> implements ResourceIdentifiable<F> {

	private Class<?> representedInterface;
	private String resourceName;
	private PrimaryIndexIdentifiable primaryIndexIdentifiable;
	private Collection<? extends SecondaryIndexIdentifiable> secondaryIndexIdentifiables;
	private String idPropertyName;
	private boolean isPartitioningResource;
	
	public ResourceIdentifiableImpl(
			Class<?> representedInterface, 
			String resourceName,
			PrimaryIndexIdentifiable primaryIndexIdentifiable,
			Collection<? extends SecondaryIndexIdentifiable> secondaryIndexIdentifiables,
			boolean isPartitioningResource,
			String idPropertyName) {
		this.representedInterface = representedInterface;
		this.resourceName = resourceName;
		this.primaryIndexIdentifiable = primaryIndexIdentifiable;
		this.secondaryIndexIdentifiables = secondaryIndexIdentifiables;
		this.isPartitioningResource = isPartitioningResource;
		this.idPropertyName = idPropertyName;
	}
	
	public String getResourceName() {
		return resourceName;
	}
	
	public Collection<? extends SecondaryIndexIdentifiable> getSecondaryIndexIdentifiables() {
		return secondaryIndexIdentifiables;
	}
	
	public String getIdPropertyName() {
		return this.idPropertyName;
	}
	@SuppressWarnings("unchecked")
	public F getId(Object instance) {
		return (F)ReflectionTools.invokeGetter(instance, idPropertyName);
	}
	
	public PrimaryIndexIdentifiable getPrimaryIndexIdentifiable() {
		return primaryIndexIdentifiable;
	}

	public boolean isPartitioningResource() {
		return isPartitioningResource;
	}

	public Class getRepresentedInterface() {
		return representedInterface;
	}
}
