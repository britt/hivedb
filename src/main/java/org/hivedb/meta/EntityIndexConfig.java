package org.hivedb.meta;

import java.util.Collection;


/**
 * 
 * For use by HiveScenario.
 * 
 * Represents a secondary index of a Hive resource. SecondaryIndexIdentifiable instances always
 * belong to a ResourceIdentifiable instance.
 * @author Andy alikuski@cafepress.com
 *
 */
public interface EntityIndexConfig
{
	Collection<Object> getIndexValues(Object entityInstance);	
	String getIndexName();
	Class<?> getIndexClass();
}