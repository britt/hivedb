package org.hivedb.meta;

import org.hivedb.util.GeneratePrimitiveValue;
import org.hivedb.util.ReflectionTools;
import org.hivedb.util.functional.Generator;

public class PrimaryIndexIdentifiableGeneratableImpl implements Generator<Object>{

	@SuppressWarnings("unchecked")
	public PrimaryIndexIdentifiableGeneratableImpl(
			ResourceIdentifiable<Object> resourceIdentifiable) {
		
		this.generator = new GeneratePrimitiveValue(ReflectionTools.getPropertyType(
				resourceIdentifiable.getRepresentedInterface(),
				resourceIdentifiable.getPrimaryIndexIdentifiable().getPrimaryKeyPropertyName()));
	}
	
	private Generator<Object> generator;
	public Object f() {
		return generator.f();
	}
}
