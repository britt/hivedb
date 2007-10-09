package org.hivedb.meta;

import org.hivedb.util.GenerateInstance;
import org.hivedb.util.PropertySetter;

public class GenerateEntityInstance<R> {
	private EntityConfig entityConfig;
	public GenerateEntityInstance(EntityConfig entityConfig) {
		this.entityConfig = entityConfig;
	}
	@SuppressWarnings("unchecked")
	public R generate(Object primaryIndexKey) {
		R instance = new GenerateInstance<R>(entityConfig.getRepresentedInterface()).generate();
		((PropertySetter)instance).set(entityConfig.getPrimaryIndexKeyPropertyName(), primaryIndexKey);
		return instance;
	}
}
