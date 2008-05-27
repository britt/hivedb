package org.hivedb.meta;

import org.hivedb.configuration.EntityConfig;
import org.hivedb.util.ReflectionTools;
import org.hivedb.util.classgen.GeneratePrimitiveValue;
import org.hivedb.util.functional.Generator;

public class PrimaryIndexKeyGenerator implements Generator<Object>{

	@SuppressWarnings("unchecked")
	public PrimaryIndexKeyGenerator(
			EntityConfig entityConfig) {
		
		this.generator = new GeneratePrimitiveValue(ReflectionTools.getPropertyType(
				entityConfig.getRepresentedInterface(),
				entityConfig.getPrimaryIndexKeyPropertyName()));
	}
	
	private Generator<?> generator;
	public Object generate() {
		return generator.generate();
	}
}
