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
}