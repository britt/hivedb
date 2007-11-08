package org.hivedb.meta;

import org.hivedb.configuration.EntityConfig;


public class EntityGeneratorImpl<F extends Object> implements EntityGenerator<F> {

	private EntityConfig entityConfig;
	public EntityGeneratorImpl(EntityConfig entityConfig) {
		this.entityConfig = entityConfig;
	}
	
	public Object generate(Object primaryIndexKey) {
		return new GenerateEntityInstance(entityConfig).generate(primaryIndexKey);
	}
}
