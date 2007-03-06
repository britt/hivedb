package org.hivedb.util.scenarioBuilder;


/**
 * 
 * For use by HiveScenario.
 * Classes used to model only resources/secondary indexes, not primary indexes, implement this interface.
 * You should only use this interface directly for inter-class secondary indexes (e.g. foo.id -> bar.id)
 * since for inner-class secondary indexes (e.g. foo.name -> foo.id) the class also has to implement
 * PrimaryIndexIndifiable, and should therefore implement the composit Identifiable.
 * 
 * Although this interface can't enforce it, you must have a one-argument constructor that takes the
 * primary index instance as an argument (the same instance that will be returned by getPrimaryIndexIdAsSecondaryIndexInstance().
 * This allows HiveScenario to construct instances.
 * 
 * @author Andy alikuski@cafepress.com
 *
 */
public interface SecondaryIndexIdentifiable
{
	/**
	 *  
	 * @return The id to be used as a secondary index key.
	 */
	Object getIdAsSecondaryIndexInstance();
	/**
	 * 
	 * @return The id to be used as the correspoding primary index key of the secondary index key.
	 * This will be the same id as getIdAsPrimaryIndexInstance() for an intra-class secondary index (e.g. foo.name -> foo.id),
	 * and will be the id from some other class's instance for an inter-class secondary index (e.g. foo.id -> bar.id).
	 */
	Object getPrimaryIndexIdAsSecondaryIndexInstance();
	/**
	 * 
	 * @return A reference to the instance identified by getPrimaryIndexIdAsSecondaryIndexInstance().
	 * This will simply return this for an intra-class secondary index (e.g. foo.name -> foo.id, so return foo),
	 * and will be the instance of a different class for an inter-class secondary index (e.g. foo.id -> bar.id, so return bar)
	 */
	PrimaryIndexIdentifiable getPrimaryIndexInstanceReference();
	
	/**
	 * 
	 * @return The name to use for the resource represented by this class. Usually this.getClass().getSimpleName() will suffice,
	 * unless you are extending a class and want to name the resource after the base class.
	 */
	String getResourceName();
	
	/**
	 * @return The string to use in naming the secondary index table, 
	 * For an intra-class secondary index, this will be the name of the field being indexed (e.g. for foo.name -> foo.id, 
	 * this should return "name" and the secondary secondary index table will be named hive_secondary_foo_name)
	 * For an inter-class secondary index, this will be the named of the id being indexed (e.g. for foo.id -> bar.id,
	 * this should return something like "barid" 
	 */
	String getSecondaryIdName();
}