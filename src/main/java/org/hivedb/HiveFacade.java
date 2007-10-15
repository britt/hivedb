package org.hivedb;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;

import org.hivedb.management.statistics.HivePerformanceStatistics;
import org.hivedb.management.statistics.PartitionKeyStatisticsDao;
import org.hivedb.meta.AccessType;
import org.hivedb.meta.Node;
import org.hivedb.meta.PartitionDimension;
import org.hivedb.meta.Resource;
import org.hivedb.meta.SecondaryIndex;
import org.hivedb.util.database.HiveDbDialect;

public interface HiveFacade {

	//Hive Attributes
	public String getUri();
	public int getRevision();
	public boolean isReadOnly();
	public HiveDbDialect getDialect();
	public PartitionKeyStatisticsDao getPartitionStatistics();
	public HivePerformanceStatistics getPerformanceStatistics();
	public boolean isPerformanceMonitoringEnabled();
	
	public void setPerformanceStatistics( HivePerformanceStatistics performanceStatistics);
	public void setPerformanceMonitoringEnabled(boolean performanceMonitoringEnabled);
	public void setHiveReadOnly(Boolean readOnly);
	
	//Hive configuration

	//PartitionDimension
	public PartitionDimension addPartitionDimension(PartitionDimension partitionDimension) throws HiveReadOnlyException;
	public Collection<PartitionDimension> getPartitionDimensions();
	public PartitionDimension getPartitionDimension(String name); 
	public PartitionDimension getPartitionDimension(final int id);
	public PartitionDimension updatePartitionDimension(PartitionDimension partitionDimension) throws HiveReadOnlyException;
	public PartitionDimension deletePartitionDimension(PartitionDimension partitionDimension) throws HiveReadOnlyException;

	//Node
	public Node addNode(PartitionDimension partitionDimension, Node node) throws HiveReadOnlyException;
	public Node updateNode(Node node) throws HiveReadOnlyException;
	public void updateNodeReadOnly(Node node, Boolean readOnly);
	public Node deleteNode(Node node) throws HiveReadOnlyException;
	
	//Resource
	public Resource addResource(String dimensionName, Resource resource) throws HiveReadOnlyException;
	public Resource updateResource(Resource resource) throws HiveReadOnlyException;
	public Resource deleteResource(Resource resource) throws HiveReadOnlyException;
	
	//Secondary Index
	public SecondaryIndex addSecondaryIndex(Resource resource, SecondaryIndex secondaryIndex) throws HiveReadOnlyException;
	public SecondaryIndex updateSecondaryIndex(SecondaryIndex secondaryIndex) throws HiveReadOnlyException;
	public SecondaryIndex deleteSecondaryIndex(SecondaryIndex secondaryIndex) throws HiveReadOnlyException;

	//Directory
	public void insertPartitionIndexKey(String partitionDimensionName, Object primaryIndexKey) throws HiveReadOnlyException;
	public void updatePartitionIndexReadOnly(String partitionDimensionName, Object primaryIndexKey, boolean isReadOnly) throws HiveReadOnlyException;
	public void deletePartitionIndexKey(String partitionDimensionName, Object secondaryIndexKey) throws HiveReadOnlyException;
	public boolean doesPartitionIndexKeyExist(String partitionDimensionName, Object primaryIndexKey);
	public boolean getReadOnlyOfPartitionIndexKey(String partitionDimensionName, Object primaryIndexKey);
	public Collection<Object> getPartitionIndexKeysOfSecondaryIndexKey( SecondaryIndex secondaryIndex, Object secondaryIndexKey);
	
	public void insertResourceId(String partitionDimensionName, String resourceName, Object id, Object primaryIndexKey) throws HiveReadOnlyException;
	public void insertResourceIds(String partitionDimensionName, String resourceName, Collection<Object> id, Object primaryIndexKey) throws HiveReadOnlyException;
	public void updatePartitionIndexKeyOfResourceId(String partitionDimensionName, String resourceName, Object resourceId, Object originalPartitionIndexKey, Object newPartitionIndexKey) throws HiveReadOnlyException;
	public void deleteResourceId(String partitionDimensionName, String resourceName, Object id) throws HiveReadOnlyException;
	public void deleteAllSecondaryIndexKeysOfResourceId( String partitionDimensionName, String resourceName, Object id) throws HiveReadOnlyException;
	public boolean doesResourceIdExist(String partitionDimensionName, String resourceName, Object id);
	public boolean getReadOnlyOfResourceId(String partitionDimensionName, String resourceName, Object id); 
	public Collection<Object> getResourceIdsOfSecondaryIndexKey( SecondaryIndex secondaryIndex, Object secondaryIndexKey);
	public Collection<Object> getResourceIdsOfPartitionKey(String partitionDimensionName, String resourceName, Object primaryIndexKey);
	
	public void insertSecondaryIndexKey(String partitionDimensionName, String resourceName, String secondaryIndexName, Object secondaryIndexKey, Object resourceId) throws HiveReadOnlyException;
	public void insertSecondaryIndexKeys(String partitionDimensionName, String resourceName,Map<SecondaryIndex, Collection<Object>> secondaryIndexValueMap, final Object resourceId) throws HiveReadOnlyException;
	public void updateResourceIdOfSecondaryIndexKey(String partitionDimensionName, String resourceName, String secondaryIndexName, Object secondaryIndexKey, Object originalResourceId, Object newResourceId) throws HiveReadOnlyException;
	public void deleteSecondaryIndexKey(String secondaryIndexName, String resourceName, String partitionDimensionName, Object secondaryIndexKey, Object resourceId) throws HiveReadOnlyException;
	public boolean doesSecondaryIndexKeyExist(String secondaryIndexName, String resourceName, String dimensionName, Object secondaryIndexKey);
	public Collection<Object> getSecondaryIndexKeysOfPartitionKey( String secondaryIndexName, String resourceName, String partitionDimensionName, Object primaryIndexKey);
	
	//Connection handling
	public Collection<Connection> getConnection(String partitionDimensionName, Object primaryIndexKey, AccessType intent) throws SQLException,HiveReadOnlyException;
	public Collection<Connection> getConnection(String secondaryIndexName, String resourceName, String dimensionName, Object secondaryIndexKey, AccessType intent) throws HiveReadOnlyException, SQLException;
	public Collection<Connection> getConnection(String resourceName, String dimensionName, Object resourceId, AccessType intent) throws HiveReadOnlyException, SQLException;

	public JdbcDaoSupportCache getJdbcDaoSupportCache(String partitionDimensionName);

}