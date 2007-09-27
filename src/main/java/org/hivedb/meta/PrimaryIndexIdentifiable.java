/**
 * 
 */
package org.hivedb.meta;


/**
 * 
 * PrimaryIndexIdentifiable describes the partition dimension and its relationship to a ResourceIdentifiable 
 * 
 * @author Andy alikuski@cafepress.com
 *
 */
public interface PrimaryIndexIdentifiable
{
	String getPrimaryKeyPropertyName();
	String getPartitionDimensionName();
	Object getPrimaryIndexKey(Object resourceInstance);
}