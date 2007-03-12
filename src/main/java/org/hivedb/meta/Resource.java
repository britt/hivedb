/**
 * HiveDB is an Open Source (LGPL) system for creating large, high-transaction-volume
 * data storage systems.
 */
package org.hivedb.meta;

import java.util.ArrayList;
import java.util.Collection;

import org.hivedb.HiveException;
import org.hivedb.util.HiveUtils;

/**
 * 
 * 
 * @author Kevin Kelm (kkelm@fortress-consulting.com)
 * @author Andy Likuski (alikuski@cafepress.com)
 */
public class Resource implements Comparable<Resource>, IdAndNameIdentifiable, Finder {
	private int id;
	private String name;
	private PartitionDimension partitionDimension;
	private Collection<SecondaryIndex> secondaryIndexes = null;

	/**
	 * 
	 * Create Constructor
	 * 
	 * @param name
	 * @param secondaryIndexes
	 */
	public Resource(String name) {
		this(Hive.NEW_OBJECT_ID, name, new ArrayList<SecondaryIndex>());
	}
	
	/**
	 * 
	 * Create Constructor
	 * 
	 * @param name
	 * @param secondaryIndexes
	 */
	public Resource(String name, Collection<SecondaryIndex> secondaryIndexes) {
		this(Hive.NEW_OBJECT_ID, name, secondaryIndexes);
	}
	
	/**
	 * 
	 * PERSISTENCE LOAD ONLY --
	 * The reference to PartitionDimension will be set by the PartitionDimension constructor.
	 * 
	 * @param id
	 * @param name
	 * @param secondaryIndexes
	 */
	public Resource(int id, String name, Collection<SecondaryIndex> secondaryIndexes) {
		super();
		this.id = id;
		this.name = name;
		this.secondaryIndexes = insetThisInstance(secondaryIndexes);		
	}
	private Collection<SecondaryIndex> insetThisInstance(Collection<SecondaryIndex> secondaryIndexes)
	{
		for (SecondaryIndex secondaryindex : secondaryIndexes)
			secondaryindex.setResource(this);
		return secondaryIndexes;
	}
	
	public int getId() {
		return id;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public PartitionDimension getPartitionDimension() {
		return partitionDimension;
	}
	public void setPartitionDimension(PartitionDimension partitionDimension) {
		this.partitionDimension = partitionDimension;
	}
	public Collection<SecondaryIndex> getSecondaryIndexes() {
		return secondaryIndexes;
	}
	public SecondaryIndex getSecondaryIndex(String secondaryIndexName) throws HiveException {
		for (SecondaryIndex secondaryIndex : getSecondaryIndexes())
			if (secondaryIndex.getName().equals(secondaryIndexName))
				return secondaryIndex;
		throw new HiveException(String.format("Secondary index %s of resource %s of partitition dimension %s not found.",
				secondaryIndexName,
				getName(),
				getPartitionDimension().getName()));
	}
	
	@SuppressWarnings("unchecked")
	public <T extends Nameable> T findByName(Class<T> forClass, String name) throws HiveException {
		return (T)getSecondaryIndex(name);
	}
	@SuppressWarnings("unchecked")
	public <T extends Nameable> Collection<T> findCollection(Class<T> forClass) {
		return (Collection<T>)getSecondaryIndexes();
	}

	/**
	 * For use by persistence layer and unit tests.  Otherwise, id should be considered immmutable.
	 * 
	 * @param id Database-generated identifier with which this instance should be updated
	 */
	public void updateId(int id) {
		this.id = id;
	}
	
	public boolean equals(Object obj)
	{
		return obj.hashCode() == hashCode();
	}
	public int hashCode() {
		return HiveUtils.makeHashCode(new Object[] {
				name, HiveUtils.makeHashCode(secondaryIndexes)
		});
	}
	
	public String toString()
	{
		return HiveUtils.toDeepFormatedString(this, 
										"Id", 					getId(), 
										"Name", 				getName(), 
										"SecondaryIndexes", 	getSecondaryIndexes());																		
	}

	public int compareTo(Resource o) {
		return getName().compareTo(o.getName());
	}
	
	public Object clone()
	{
		return new Resource(name, secondaryIndexes);
	}
}
