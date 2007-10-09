package org.hivedb.meta;

public interface EntityGenerator<F extends Object> {
	/**
	 *  Generate a new ResourceIdentifiable instance from a prototype instance.
	 * @param primaryIndexIdentifiable - The PrimaryIndexIdentifiable of the generated instance. This will probably also be a generated instance.
	 * @return
	 */
	Object generate(Object primaryIndexKey);
}
