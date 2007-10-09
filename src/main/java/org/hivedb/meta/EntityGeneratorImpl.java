package org.hivedb.meta;


public class EntityGeneratorImpl<F extends Object> implements EntityGenerator<F> {

	private EntityConfig<F> entityConfig;
	public EntityGeneratorImpl(EntityConfig<F> entityConfig) {
		this.entityConfig = entityConfig;
	}
	
	public Object generate(Object primaryIndexKey) {
		return new GenerateEntityInstance(entityConfig).generate(primaryIndexKey);
	}
}
