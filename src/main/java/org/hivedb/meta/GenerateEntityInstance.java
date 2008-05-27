package org.hivedb.meta;

import org.hivedb.configuration.EntityConfig;
import org.hivedb.util.PropertyAccessor;
import org.hivedb.util.classgen.GenerateInstance;

public class GenerateEntityInstance<R> {
	private EntityConfig entityConfig;
	public GenerateEntityInstance(EntityConfig entityConfig) {
		this.entityConfig = entityConfig;
	}
	@SuppressWarnings("unchecked")
	public R generate(Object primaryIndexKey) {
		R instance = new GenerateInstance<R>((Class<R>) entityConfig.getRepresentedInterface()).generate();
		((PropertyAccessor)instance).set(entityConfig.getPrimaryIndexKeyPropertyName(), primaryIndexKey);
		return instance;
	}
}
