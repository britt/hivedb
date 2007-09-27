package org.hivedb.meta;

import java.util.Collection;

public class ResourceIdentifiableGeneratableImpl<F extends Object> implements ResourceIdentifiableGeneratable<F> {

	public ResourceIdentifiableGeneratableImpl(ResourceIdentifiable<F> resourceIdentifiable) {
		this.resourceIdentifiable = resourceIdentifiable;
	}
	
	public Object generate(Object primaryIndexKey) {
		return new GenerateResourceInstance(resourceIdentifiable).generate(primaryIndexKey);
	}

	ResourceIdentifiable<F> resourceIdentifiable;
	public F getId(Object instance) {
		return resourceIdentifiable.getId(instance);
	}

	public PrimaryIndexIdentifiable getPrimaryIndexIdentifiable() {
		return resourceIdentifiable.getPrimaryIndexIdentifiable();
	}

	public String getResourceName() {
		return resourceIdentifiable.getResourceName();
	}

	public Collection<? extends SecondaryIndexIdentifiable> getSecondaryIndexIdentifiables() {
		return resourceIdentifiable.getSecondaryIndexIdentifiables();
	}

	public boolean isPartitioningResource() {
		return resourceIdentifiable.isPartitioningResource();
	}

	public Class getRepresentedInterface() {
		return resourceIdentifiable.getRepresentedInterface();
	}

	public String getIdPropertyName() {
		return resourceIdentifiable.getIdPropertyName();
	}
}
