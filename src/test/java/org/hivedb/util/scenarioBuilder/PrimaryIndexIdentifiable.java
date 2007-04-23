/**
 * 
 */
package org.hivedb.util.scenarioBuilder;

import java.util.Collection;

/**
 * 
 * This interface is used by a class that is a primary index in a Hive database.
 * 
 * @author Andy alikuski@cafepress.com
 *
 */
public interface PrimaryIndexIdentifiable
{
	/**
	 *  Used to generate a new instance of this PrimaryIndexIdentifiable based on a prototype instance
	 * @return
	 */
	PrimaryIndexIdentifiable generate();
	
	/**
	 *  Returns a list of prototype ResourceIdentifiable instances whose construct method will be used to create real instances.
	 * @return
	 */
	Collection<ResourceIdentifiable> getResourceIdentifiables();
	
	/**
	 *  
	 * @return The id to be used as a primary index key. Make sure that this never returns null, use the default value instead.
	 */
	Object getPrimaryIndexKey();
	
	/**
	 * 
	 * @return The name to use for the partition dimension represented by this class. Usually this.getClass().getSimpleName() will suffice,
	 * unless you are extending a class and want to name the resource after the base class.
	 */
	String getPartitionDimensionName();
}