/**
 * HiveDB is an Open Source (LGPL) system for creating large, high-transaction-volume
 * data storage systems.
 */
package org.hivedb.meta;

import java.util.ArrayList;
import java.util.Collection;

import org.hivedb.Hive;
import org.hivedb.HiveRuntimeException;
import org.hivedb.meta.persistence.ColumnInfo;
import org.hivedb.util.HiveUtils;

/**
 * An index entity in the Hive.
 * 
 * @author Kevin Kelm (kkelm@fortress-consulting.com)
 * @author Andy Likuski (alikuski@cafepress.com)
 */
public class Resource implements Comparable<Resource>, IdAndNameIdentifiable<Integer>, Finder {
	private int id;
	private PartitionDimension partitionDimension;
	private Collection<SecondaryIndex> secondaryIndexes = null;
	private ColumnInfo columnInfo;
	private ResourceIndex idIndex;
	private boolean isPartitioningResource;

	/**
	 * 
	 * Create Constructor
	 * 
	 * @param name
	 * @param secondaryIndexes
	 */
	public Resource(String name, int columnType, boolean isPartitioningResource) {
		this(Hive.NEW_OBJECT_ID, name, columnType, isPartitioningResource, new ArrayList<SecondaryIndex>());
	}
	
	/**
	 * 
	 * Create Constructor
	 * 
	 * @param name
	 * @param secondaryIndexes
	 */
	public Resource(String name, int columnType, boolean isPartitioningResource, Collection<SecondaryIndex> secondaryIndexes) {
		this(Hive.NEW_OBJECT_ID, name, columnType, isPartitioningResource, secondaryIndexes);
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
	public Resource(int id, String name, int columnType, boolean isPartitioningResource, Collection<SecondaryIndex> secondaryIndexes) {
		this.id = id;
		this.columnInfo = new ColumnInfo(name, columnType);
		this.isPartitioningResource = isPartitioningResource;
		this.secondaryIndexes = insetThisInstance(secondaryIndexes);		
		this.idIndex = new ResourceIndex(name, columnType);
		idIndex.setResource(this);
	}
	private Collection<SecondaryIndex> insetThisInstance(Collection<SecondaryIndex> secondaryIndexes)
	{
		for (SecondaryIndex secondaryindex : secondaryIndexes)
			secondaryindex.setResource(this);
		return secondaryIndexes;
	}
	
	public Integer getId() {
		return id;
	}
	public String getName() {
		return columnInfo.getName();
	}
	public void setName(String name) {
		this.columnInfo = new ColumnInfo(name, columnInfo.getColumnType());
	}
	public PartitionDimension getPartitionDimension() {
		return partitionDimension;
	}
	public boolean isPartitioningResource() {
		return isPartitioningResource;
	}
	public void setIsPartitioningResource(boolean value) {
		isPartitioningResource = value;
	}
	public void setPartitionDimension(PartitionDimension partitionDimension) {
		this.partitionDimension = partitionDimension;
	}
	public Collection<SecondaryIndex> getSecondaryIndexes() {
		return secondaryIndexes;
	}
	public SecondaryIndex getSecondaryIndex(String secondaryIndexName) {
		for (SecondaryIndex secondaryIndex : getSecondaryIndexes())
			if (secondaryIndex.getName().equalsIgnoreCase(secondaryIndexName))
				return secondaryIndex;
		throw new HiveRuntimeException(String.format("Secondary index %s of resource %s of partitition dimension %s not found.",
				secondaryIndexName,
				getName(),
				getPartitionDimension().getName()));
	}
	
	@SuppressWarnings("unchecked")
	public <T extends Nameable> T findByName(Class<T> forClass, String name) {
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
				getName(), HiveUtils.makeHashCode(secondaryIndexes)
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
		return new Resource(columnInfo.getName(), columnInfo.getColumnType(), isPartitioningResource, secondaryIndexes);
	}
	
	
	public ResourceIndex getIdIndex() {
		return idIndex;
	}
	
	public int getColumnType() {
		return getIdIndex().getColumnInfo().getColumnType();
	}

	public void setSecondaryIndexes(Collection<SecondaryIndex> secondaryIndexes) {
		this.secondaryIndexes = secondaryIndexes;
	}

	public void setId(Integer field) {
		this.id = field;
	}
}
