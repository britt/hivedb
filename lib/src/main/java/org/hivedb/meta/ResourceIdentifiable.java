package org.hivedb.meta;

import java.util.Collection;

/**
 *  Since a hive resource has one or more secondary indexes a ResourceIdentifiable
 * will have at least one SecondaryIndexIdentifiable instance for each of its secondary indexes.
 * Additionally if the secondary index represents a many-to-many table relationship, then
 * the ResourceIdentifiable will have one or more SecondaryIndexIdentifiables PER secondary index.
 * The method getSecondaryIndexIdentifiables() flattens this possible two-dimensional
 * collection of secondary indexes into one dimension.
 * 
 * @author andylikuski
 *
 */
public interface ResourceIdentifiable {
	
	/**
	 *  Generate a new ResourceIdentifiable instance from a prototype instance (an instance constructed with them no-arg constructor)
	 * @param primaryIndexIdentifiable - The PrimaryIndexIdentifiable of the generated instance. This will probably also be a generated instance.
	 * @return
	 */
	ResourceIdentifiable generate(PrimaryIndexIdentifiable primaryIndexIdentifiable);
	Collection<SecondaryIndexIdentifiable> getSecondaryIndexIdentifiables();
	PrimaryIndexIdentifiable getPrimaryIndexIdentifiable();
	String getResourceName();
	Number getId();
	<T> Class<T> getRepresentedClass();
}
