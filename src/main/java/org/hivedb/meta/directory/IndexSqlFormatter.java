package org.hivedb.meta.directory;

import org.hivedb.meta.PartitionDimension;
import org.hivedb.meta.Resource;
import org.hivedb.meta.ResourceIndex;
import org.hivedb.meta.SecondaryIndex;
import org.hivedb.meta.persistence.IndexSchema;

/***
 * Methods for generating SQL strings used to read and write from the HiveDB directory.
 * @author bcrawford
 *
 */
public class IndexSqlFormatter {
	/**
	 * 
	 * Primary index methods
	 * 
	 */
	public String insertPrimaryIndexKey(PartitionDimension partitionDimension) {
		return String.format(
				"insert into %s (id, node, read_only) values(?, ?, 0)", 
				IndexSchema.getPrimaryIndexTableName(partitionDimension));
	}
	
	public String selectKeySemaphoreOfPrimaryIndexKey(PartitionDimension partitionDimension) {
		return String.format("select node,read_only from %s where id = ?", IndexSchema.getPrimaryIndexTableName(partitionDimension)); 
	}
	
	public String selectResourceIdsOfPrimaryIndexKey(ResourceIndex resourceIndex) {
		return String.format("select id from %s where pkey = ?", IndexSchema.getResourceIndexTableName(resourceIndex.getResource()));
	}

	public String checkExistenceOfPrimaryKey(PartitionDimension partitionDimension) {
		return String.format("select id from %s where id =  ?", IndexSchema.getPrimaryIndexTableName(partitionDimension));
	}
	
	public String updateReadOnlyOfPrimaryIndexKey(PartitionDimension partitionDimension) {
		return String.format("update %s set read_only = ? where id = ?", IndexSchema.getPrimaryIndexTableName(partitionDimension));
	}
	
	public String deletePrimaryIndexKey(PartitionDimension partitionDimension) {
		return String.format("delete from %s where id = ?", IndexSchema.getPrimaryIndexTableName(partitionDimension));
	}
	
	/**
	 * 
	 * Secondary index methods
	 * 
	 */
	public String insertSecondaryIndexKey(SecondaryIndex secondaryIndex) {
		return String.format("insert into %s (id, pkey) values(?, ?)", IndexSchema.getSecondaryIndexTableName(secondaryIndex));
	}
	
	public String selectSecondaryIndexKeysOfPrimaryKey(SecondaryIndex secondaryIndex) {
		if (ResourceIndex.class.isInstance(secondaryIndex))
			// index of a resource
			return selectSecondaryIndexKeyOfResourceId(secondaryIndex);
		else if (secondaryIndex.getResource().isPartitioningResource())
		 	// secondary index of a resource that is also the partition dimension
		 	return String.format(
				"select s.id from %s s where s.pkey = ?", 
				IndexSchema.getSecondaryIndexTableName(secondaryIndex));
		else
			// secondary index of a resource with a different partition dimension
			return String.format(
				"select s.id from %s p join %s r on r.pkey = p.id join %s s on s.pkey = r.id where p.id = ?", 
				IndexSchema.getPrimaryIndexTableName(secondaryIndex.getResource().getPartitionDimension()),
				IndexSchema.getResourceIndexTableName(secondaryIndex.getResource()),
				IndexSchema.getSecondaryIndexTableName(secondaryIndex));
	}
	
	public String selectKeySemaphoresOfSecondaryIndexKey(SecondaryIndex secondaryIndex) {
		if (ResourceIndex.class.isInstance(secondaryIndex))		
			// index of a resource
			return String.format(
				"select p.node,p.read_only from %s p join %s r on r.pkey = p.id where r.id = ?", 
				IndexSchema.getPrimaryIndexTableName(secondaryIndex.getResource().getPartitionDimension()),
				IndexSchema.getSecondaryIndexTableName(secondaryIndex.getResource().getIdIndex()));
		else if (secondaryIndex.getResource().isPartitioningResource())
			 // secondary index of a resource that is also the partition dimension
			 return String.format(
				"select p.node,p.read_only from %s p join %s s on s.pkey = p.id where s.id = ?", 
				IndexSchema.getPrimaryIndexTableName(secondaryIndex.getResource().getPartitionDimension()),
				IndexSchema.getSecondaryIndexTableName(secondaryIndex));
		else 
			// secondary index of a resource that is not also the partition dimension
			return String.format(
				"select p.node,p.read_only from %s p join %s r on r.pkey = p.id join %s s on s.pkey = r.id where s.id = ?", 
				IndexSchema.getPrimaryIndexTableName(secondaryIndex.getResource().getPartitionDimension()),
				IndexSchema.getResourceIndexTableName(secondaryIndex.getResource()),
				IndexSchema.getSecondaryIndexTableName(secondaryIndex));
	}
	
	public String selectPrimaryIndexKeysOfSecondaryIndexKey( SecondaryIndex secondaryIndex) {
		if (ResourceIndex.class.isInstance(secondaryIndex))
			// index of a resource
			return 	String.format(
				"select p.id from %s p join %s r on r.pkey = p.id where r.id = ?", 
				IndexSchema.getPrimaryIndexTableName(secondaryIndex.getResource().getPartitionDimension()),
				IndexSchema.getResourceIndexTableName(secondaryIndex.getResource()));
		else if (secondaryIndex.getResource().isPartitioningResource())
			// secondary index of a resource that is also the partition dimension
			return String.format(
				"select p.id from %s p join %s s on s.pkey = p.id where s.id = ?", 
				IndexSchema.getPrimaryIndexTableName(secondaryIndex.getResource().getPartitionDimension()),
				IndexSchema.getSecondaryIndexTableName(secondaryIndex));
		else
			// secondary index of a resource that is not also the partition dimension
			return String.format(
				"select p.id from %s p join %s r on r.pkey = p.id join %s s on s.pkey = r.id where s.id = ?", 
				IndexSchema.getPrimaryIndexTableName(secondaryIndex.getResource().getPartitionDimension()),
				IndexSchema.getResourceIndexTableName(secondaryIndex.getResource()),
				IndexSchema.getSecondaryIndexTableName(secondaryIndex));
	}
	
	public String checkExistenceOfSecondaryIndexSql( SecondaryIndex secondaryIndex) {
		return String.format("select id from %s where id = ? and pkey = ? limit 1", IndexSchema.getSecondaryIndexTableName(secondaryIndex));
	}
	
	public String checkExistenceOfResourceIndexSql( ResourceIndex secondaryIndex) {
		return String.format("select id from %s where id = ? limit 1", IndexSchema.getResourceIndexTableName(secondaryIndex.getResource()));
	}
	
	public String updateSecondaryIndexKey( SecondaryIndex secondaryIndex) {
		return String.format("update %s set pkey = ? where id = ? and pkey = ?", IndexSchema.getSecondaryIndexTableName(secondaryIndex));
	}
	
	public String deleteAllSecondaryIndexKeysForResourceId(SecondaryIndex secondaryIndex) {
		return String.format("delete from %s where pkey = ?", IndexSchema.getSecondaryIndexTableName(secondaryIndex));
	}
	
	public String deleteSingleSecondaryIndexKey(SecondaryIndex secondaryIndex) {
		return String.format("delete from %s where id =? and pkey = ?", IndexSchema.getSecondaryIndexTableName(secondaryIndex));
	}
	
	/***
	 * Resource methods
	 */
	public String insertResourceId(Resource resource) {
		return String.format("insert into %s (id, pkey) values(?, ?)", IndexSchema.getResourceIndexTableName(resource));
	}
	
	public String selectResourceIdsOfSecondaryIndexKey(SecondaryIndex secondaryIndex) {
		final Resource resource = secondaryIndex.getResource();
		if (resource.isPartitioningResource())
			return selectPrimaryIndexKeysOfSecondaryIndexKey(secondaryIndex);
		return String.format(
				"select r.id from %s r join %s s on s.pkey = r.id where s.id = ?",
				IndexSchema.getResourceIndexTableName(resource),
				IndexSchema.getSecondaryIndexTableName(secondaryIndex));
	}
	
	public String selectKeySemaphoresOfResourceId(Resource resource) {
		return String.format(
				"select p.node,p.read_only from %s p join %s r on r.pkey = p.id where r.id = ?", 
				IndexSchema.getPrimaryIndexTableName(resource.getPartitionDimension()),
				IndexSchema.getResourceIndexTableName(resource));
	}
	
	public String selectPrimaryIndexKeysOfResourceId(Resource resource) {
		return String.format(
				"select p.id from %s p join %s r on r.pkey = p.id where r.id = ?", 
				IndexSchema.getPrimaryIndexTableName(resource.getPartitionDimension()),
				IndexSchema.getResourceIndexTableName(resource));
	}
	
	public String selectSecondaryIndexKeyOfResourceId(SecondaryIndex secondaryIndex) {
		return String.format(
				"select s.id from %s s where s.pkey = ?", 
				IndexSchema.getSecondaryIndexTableName(secondaryIndex),
				IndexSchema.getResourceIndexTableName(secondaryIndex.getResource()));
	}
	
	public String updateResourceId( Resource resource) {
		return String.format("update %s set pkey = ? where id = ?", IndexSchema.getResourceIndexTableName(resource));
	}
	
	public String deleteResourceId(Resource resource) {
		return String.format("delete from %s where id = ?", IndexSchema.getResourceIndexTableName(resource));
	}

}
