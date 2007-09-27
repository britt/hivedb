package org.hivedb.meta;


/**
 * 
 * For use by HiveScenario.
 * 
 * Represents a secondary index of a Hive resource. SecondaryIndexIdentifiable instances always
 * belong to a ResourceIdentifiable instance.
 * @author Andy alikuski@cafepress.com
 *
 */
public interface SecondaryIndexIdentifiable
{
	/**
	 *  Returns the single or collection of values that represent secondary index keys of a secondary index.
	 *  If isManyToOneMultiplicity() is true then a Collection will be returned, and otherwise a singular value.
	 * @param resourceInstance
	 * @return
	 */
	Object getSecondaryIndexValue(Object resourceInstance);	
	String getSecondaryIndexKeyPropertyName();
	boolean isManyToOneMultiplicity();
}