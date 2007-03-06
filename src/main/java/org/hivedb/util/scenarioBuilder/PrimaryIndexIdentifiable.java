/**
 * 
 */
package org.hivedb.util.scenarioBuilder;

/**
 * 
 * For use by HiveScenario.
 * Classes used to model only primary indexes, not resources/secondary indexes implement this interface.
 * 
 * Although this interface can't enforce it, you must have a no-argument constructor on your implementor.
 * This allows HiveScenario to construct instances.
 * 
 * @author Andy alikuski@cafepress.com
 *
 */
public interface PrimaryIndexIdentifiable
{
	/**
	 *  
	 * @return The id to be used as a primary index key.
	 */
	Object getIdAsPrimaryIndexInstance();
	
	/**
	 * 
	 * @return The name to use for the partition dimension represented by this class. Usually this.getClass().getSimpleName() will suffice,
	 * unless you are extending a class and want to name the resource after the base class.
	 */
	String getPartitionDimensionName();
}