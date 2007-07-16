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
	 * This is real constructor of the class. HiveScenario will create an instance with the no argument constructor and then call this method, creating a new instance.
	 * @param resourceIdentifiable - The ResourceIdentifiable instance by the generated instance, probably also a generated instance
	 * @param id - The primary key of this secondary index instance.
	 * @return
	 */
	SecondaryIndexIdentifiable generate(ResourceIdentifiable resourceIdentifiable, Object id);
	/**
	 *  
	 * @return The secondary index key represented by this instance. Make sure that this never returns null, use the default value instead.
	 * 
	 * For an intra-class secondary index where Foo is the primary index class and resource class, for the secondary index foo.name to foo.id then this key will be foo.getName()
	 * For an inter-class secondary index, where Foo is the primary index class and Bar the resource class, for the secondary index bar.id to foo.id this key will be bar.getId()
	 */
	Object getSecondaryIndexKey();
	
	/**
	 * 
	 * @return The name to use for the resource represented by this class. Usually this.getClass().getSimpleName() will suffice,
	 * unless you are extending a class and want to name the resource after the base class.
	 */
	ResourceIdentifiable getResourceIdentifiable();
	Object getRepresentedResourceFieldValue();
	String getSecondaryIndexColumnName();
	
	/**
	 * @return The name of the column of the secondary index. This name is used to form the SecondaryIndex's full name which is structured as "resource.name_column_name"
	 * 
	 * This name could be calculated, but it's clearer to restate it here.
	 * 
	 * For an intra-class secondary index where Foo is the primary index class and resource class, 
	 * the secondary index that maps foo.name to foo.id would have a column name of "name" and a secondary index name of "foo.name"
	 * For an inter-class secondary index, where Foo is the primary index class and bar the resource class,
	 * the secondary index that maps bar.id to foo.id would have a column name of "id" and secondary index name of "bar.id"
	 */
	String getSecondaryIndexName();
	
	<T> Class<T> getRepresentedClass();
}