package org.hivedb.meta;

import org.hivedb.util.GeneratePrimitiveValue;
import org.hivedb.util.ReflectionTools;
import org.hivedb.util.functional.Generator;

public class PrimaryIndexKeyGenerator implements Generator<Object>{

	@SuppressWarnings("unchecked")
	public PrimaryIndexKeyGenerator(
			EntityConfig<Object> entityConfig) {
		
		this.generator = new GeneratePrimitiveValue(ReflectionTools.getPropertyType(
				entityConfig.getRepresentedInterface(),
				entityConfig.getPrimaryIndexKeyPropertyName()));
	}
	
	private Generator<Object> generator;
	public Object generate() {
		return generator.generate();
	}
}
