package org.hivedb.meta;

import org.hivedb.util.GenerateInstance;
import org.hivedb.util.GenerateInstance.PropertySetter;

public class GenerateResourceInstance<R> {
	private ResourceIdentifiable resourceIdentifiable;
	public GenerateResourceInstance(ResourceIdentifiable resourceIdentifiable) {
		this.resourceIdentifiable = resourceIdentifiable;
	}
	@SuppressWarnings("unchecked")
	public R generate(Object primaryIndexKey) {
		R instance = new GenerateInstance<R>(resourceIdentifiable.getRepresentedInterface()).f();
		((PropertySetter)instance).set(resourceIdentifiable.getPrimaryIndexIdentifiable().getPrimaryKeyPropertyName(), primaryIndexKey);
		return instance;
	}
}
