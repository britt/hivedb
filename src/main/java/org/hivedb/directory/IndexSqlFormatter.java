package org.hivedb.directory;

import org.hivedb.ResourceIndex;
import org.hivedb.SecondaryIndex;
import org.hivedb.util.database.Schemas;
import org.hivedb.PartitionDimension;
import org.hivedb.Resource;
import org.hivedb.configuration.HiveConfiguration;

/***
 * Methods for generating SQL strings used to read and write from the HiveDB directory.
 * @author bcrawford
 *
 */
public class IndexSqlFormatter {
  private HiveConfiguration config;

  public IndexSqlFormatter(HiveConfiguration config) {
    this.config = config;
  }

  /**
	 * 
	 * Primary index methods
	 * 
	 */
	public String insertPrimaryIndexKey(PartitionDimension partitionDimension) {
		return String.format(
				"insert into %s (id, node, status) values(?, ?, 0)", 
				Schemas.getPrimaryIndexTableName(partitionDimension));
	}
	
	public String selectKeySemaphoreOfPrimaryIndexKey(PartitionDimension partitionDimension) {
		return String.format("select id,node,status from %s where id = ?", Schemas.getPrimaryIndexTableName(partitionDimension));
	}
	
	public String selectResourceIdsOfPrimaryIndexKey(ResourceIndex resourceIndex) {
		return String.format("select id from %s where pkey = ?", Schemas.getResourceIndexTableName(resourceIndex.getResource()));
	}

	public String checkExistenceOfPrimaryKey(PartitionDimension partitionDimension) {
		return String.format("select id from %s where id =  ?", Schemas.getPrimaryIndexTableName(partitionDimension));
	}
	
	public String updateReadOnlyOfPrimaryIndexKey(PartitionDimension partitionDimension) {
		return String.format("update %s set status = ? where id = ?", Schemas.getPrimaryIndexTableName(partitionDimension));
	}
	
	public String deletePrimaryIndexKey(PartitionDimension partitionDimension) {
		return String.format("delete from %s where id = ?", Schemas.getPrimaryIndexTableName(partitionDimension));
	}
	
	/**
	 * 
	 * Secondary index methods
	 * 
	 */
	public String insertSecondaryIndexKey(SecondaryIndex secondaryIndex) {
		return String.format("insert into %s (id, pkey) values(?, ?)", Schemas.getSecondaryIndexTableName(secondaryIndex));
	}
	
	public String selectSecondaryIndexKeysOfPrimaryKey(SecondaryIndex secondaryIndex) {
		if (ResourceIndex.class.isInstance(secondaryIndex))
			// index of a resource
			return selectSecondaryIndexKeyOfResourceId(secondaryIndex);
		else if (secondaryIndex.getResource().isPartitioningResource())
		 	// secondary index of a resource that is also the partition dimension
		 	return String.format(
				"select s.id from %s s where s.pkey = ?", 
				Schemas.getSecondaryIndexTableName(secondaryIndex));
		else
			// secondary index of a resource with a different partition dimension
			return String.format(
				"select s.id from %s p join %s r on r.pkey = p.id join %s s on s.pkey = r.id where p.id = ?", 
				Schemas.getPrimaryIndexTableName(config.getPartitionDimension()),
				Schemas.getResourceIndexTableName(secondaryIndex.getResource()),
				Schemas.getSecondaryIndexTableName(secondaryIndex));
	}
	
	public String selectKeySemaphoresOfSecondaryIndexKey(SecondaryIndex secondaryIndex) {
		if (ResourceIndex.class.isInstance(secondaryIndex))		
			// index of a resource
			return String.format(
				"select distinct r.id as id,p.node,p.status from %s p join %s r on r.pkey = p.id where r.id = ?",
				Schemas.getPrimaryIndexTableName(config.getPartitionDimension()),
				Schemas.getSecondaryIndexTableName(secondaryIndex.getResource().getIdIndex()));
		else if (secondaryIndex.getResource().isPartitioningResource())
			 // secondary index of a resource that is also the partition dimension
			 return String.format(
				"select distinct s.id as id,p.node,p.status from %s p join %s s on s.pkey = p.id where s.id = ?",
				Schemas.getPrimaryIndexTableName(config.getPartitionDimension()),
				Schemas.getSecondaryIndexTableName(secondaryIndex));
		else 
			// secondary index of a resource that is not also the partition dimension
			return String.format(
				"select distinct s.id as id,p.node,p.status from %s p join %s r on r.pkey = p.id join %s s on s.pkey = r.id where s.id = ?", 
				Schemas.getPrimaryIndexTableName(config.getPartitionDimension()),
				Schemas.getResourceIndexTableName(secondaryIndex.getResource()),
				Schemas.getSecondaryIndexTableName(secondaryIndex));
	}
	
	public String selectPrimaryIndexKeysOfSecondaryIndexKey( SecondaryIndex secondaryIndex) {
		if (ResourceIndex.class.isInstance(secondaryIndex))
			// index of a resource
			return 	String.format(
				"select p.id from %s p join %s r on r.pkey = p.id where r.id = ?", 
				Schemas.getPrimaryIndexTableName(config.getPartitionDimension()),
				Schemas.getResourceIndexTableName(secondaryIndex.getResource()));
		else if (secondaryIndex.getResource().isPartitioningResource())
			// secondary index of a resource that is also the partition dimension
			return String.format(
				"select p.id from %s p join %s s on s.pkey = p.id where s.id = ?", 
				Schemas.getPrimaryIndexTableName(config.getPartitionDimension()),
				Schemas.getSecondaryIndexTableName(secondaryIndex));
		else
			// secondary index of a resource that is not also the partition dimension
			return String.format(
				"select p.id from %s p join %s r on r.pkey = p.id join %s s on s.pkey = r.id where s.id = ?", 
				Schemas.getPrimaryIndexTableName(config.getPartitionDimension()),
				Schemas.getResourceIndexTableName(secondaryIndex.getResource()),
				Schemas.getSecondaryIndexTableName(secondaryIndex));
	}
	
	public String checkExistenceOfSecondaryIndexSql( SecondaryIndex secondaryIndex) {
		return String.format("select id from %s where id = ? and pkey = ? limit 1", Schemas.getSecondaryIndexTableName(secondaryIndex));
	}
	
	public String checkExistenceOfResourceIndexSql( ResourceIndex secondaryIndex) {
		return String.format("select id from %s where id = ? limit 1", Schemas.getResourceIndexTableName(secondaryIndex.getResource()));
	}
	
	public String updateSecondaryIndexKey( SecondaryIndex secondaryIndex) {
		return String.format("update %s set pkey = ? where id = ? and pkey = ?", Schemas.getSecondaryIndexTableName(secondaryIndex));
	}
	
	public String deleteAllSecondaryIndexKeysForResourceId(SecondaryIndex secondaryIndex) {
		return String.format("delete from %s where pkey = ?", Schemas.getSecondaryIndexTableName(secondaryIndex));
	}
	
	public String deleteSingleSecondaryIndexKey(SecondaryIndex secondaryIndex) {
		return String.format("delete from %s where id =? and pkey = ?", Schemas.getSecondaryIndexTableName(secondaryIndex));
	}
	
	/***
	 * Resource methods
	 */
	public String insertResourceId(Resource resource) {
		return String.format("insert into %s (id, pkey) values(?, ?)", Schemas.getResourceIndexTableName(resource));
	}
	
	public String selectResourceIdsOfSecondaryIndexKey(SecondaryIndex secondaryIndex) {
		final Resource resource = secondaryIndex.getResource();
		if (resource.isPartitioningResource())
			return selectPrimaryIndexKeysOfSecondaryIndexKey(secondaryIndex);
		return String.format(
				"select r.id from %s r join %s s on s.pkey = r.id where s.id = ?",
				Schemas.getResourceIndexTableName(resource),
				Schemas.getSecondaryIndexTableName(secondaryIndex));
	}
	
	public String selectKeySemaphoresOfResourceId(Resource resource) {
		return String.format(
				"select r.id as id,p.node,p.status from %s p join %s r on r.pkey = p.id where r.id = ?",
				Schemas.getPrimaryIndexTableName(config.getPartitionDimension()),
				Schemas.getResourceIndexTableName(resource));
	}
	
	public String selectPrimaryIndexKeysOfResourceId(Resource resource) {
		return String.format(
				"select p.id from %s p join %s r on r.pkey = p.id where r.id = ?", 
				Schemas.getPrimaryIndexTableName(config.getPartitionDimension()),
				Schemas.getResourceIndexTableName(resource));
	}
	
	public String selectSecondaryIndexKeyOfResourceId(SecondaryIndex secondaryIndex) {
		return String.format(
				"select s.id from %s s where s.pkey = ?", 
				Schemas.getSecondaryIndexTableName(secondaryIndex),
				Schemas.getResourceIndexTableName(secondaryIndex.getResource()));
	}
	
	public String updateResourceId( Resource resource) {
		return String.format("update %s set pkey = ? where id = ?", Schemas.getResourceIndexTableName(resource));
	}
	
	public String deleteResourceId(Resource resource) {
		return String.format("delete from %s where id = ?", Schemas.getResourceIndexTableName(resource));
	}
	
	public String selectForUpdateLock(String table, String column) {
		return String.format("select * from %s where %s = ? for update", table, column);
	}
	
	public String selectCompositeKeyForUpdateLock(String table, String column1, String column2) {
		return String.format("select * from %s where %s = ? and %s = ? for update", table, column1, column2);
	}
}
