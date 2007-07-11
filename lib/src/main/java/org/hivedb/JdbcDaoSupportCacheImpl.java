package org.hivedb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.dbcp.BasicDataSource;
import org.hivedb.management.statistics.HivePerformanceStatistics;
import org.hivedb.meta.AccessType;
import org.hivedb.meta.Node;
import org.hivedb.meta.NodeResolver;
import org.hivedb.meta.NodeSemaphore;
import org.hivedb.meta.SecondaryIndex;
import org.hivedb.meta.persistence.DataSourceProvider;
import org.hivedb.util.HiveUtils;
import org.hivedb.util.functional.Filter;
import org.hivedb.util.functional.Unary;
import org.springframework.jdbc.core.simple.SimpleJdbcDaoSupport;

/**
 * @author Britt Crawford (bcrawford@cafepress.com)
 *
 */
public class JdbcDaoSupportCacheImpl implements JdbcDaoSupportCache, Synchronizeable{
	private Hive hive;
	private String partitionDimension;
	private Map<Integer, SimpleJdbcDaoSupport> jdbcDaoSupports;
	private HivePerformanceStatistics stats;
	private NodeResolver directory;
	private DataSourceProvider dataSourceProvider;
	
	public JdbcDaoSupportCacheImpl(String partitionDimension, Hive hive, NodeResolver directory, DataSourceProvider dataSourceProvider) {
		this(partitionDimension, hive, directory, dataSourceProvider, null);
	}
	
	public JdbcDaoSupportCacheImpl(String partitionDimension, Hive hive, NodeResolver directory,  DataSourceProvider dataSourceProvider, HivePerformanceStatistics stats) {
		this.partitionDimension = partitionDimension;
		this.hive = hive;
		this.jdbcDaoSupports = new ConcurrentHashMap<Integer, SimpleJdbcDaoSupport>();
		this.directory = directory;
		this.stats = stats;
		this.dataSourceProvider = dataSourceProvider;
		sync();
	}
	
	/**
	 * Synchronize the cached SimpleJdbcDaoSupports with the state of the hive.  
	 * This method destroys and recreates all SimpleJdbcDaoSupport in the cache.
	 * @throws HiveException
	 */
	public void sync() {
		jdbcDaoSupports.clear();
		for(Node node : hive.getPartitionDimension(partitionDimension).getNodeGroup().getNodes()) {
			jdbcDaoSupports.put(hash(node.getId(), AccessType.Read), new DataNodeJdbcDaoSupport((BasicDataSource) dataSourceProvider.getDataSource(node.getUri()), true));
			if( !hive.isReadOnly() && !node.isReadOnly() )
				addDataSource(node.getId(), AccessType.ReadWrite);
		}
	}
	
	private SimpleJdbcDaoSupport addDataSource(Integer nodeId, AccessType intention) {
		Node node = hive.getPartitionDimension(partitionDimension).getNodeGroup().getNode(nodeId);
		jdbcDaoSupports.put(hash(nodeId, intention), new DataNodeJdbcDaoSupport((BasicDataSource) dataSourceProvider.getDataSource(node.getUri())));
		return jdbcDaoSupports.get(hash(nodeId, intention));
	}
	
	private SimpleJdbcDaoSupport get(NodeSemaphore semaphore, AccessType intention) throws HiveReadOnlyException { 
		Node node = null;
		try {
			node = hive.getPartitionDimension(partitionDimension).getNodeGroup().getNode(semaphore.getId());
		} catch (HiveRuntimeException e) {
			//failure
			countFailure();
			throw e;
		}
		
		if(intention == AccessType.ReadWrite && (hive.isReadOnly() || node.isReadOnly() || semaphore.isReadOnly())){
			//failure
			countFailure();
			throw new HiveReadOnlyException("This partition key cannot be written to at this time.");
		}
		else if( jdbcDaoSupports.containsKey(hash(semaphore.getId(), intention))){
			// success case
			countSuccess(intention);
			return jdbcDaoSupports.get(hash(semaphore.getId(), intention));
		}
		else {
			try {
			SimpleJdbcDaoSupport dao = addDataSource(semaphore.getId(), intention);
			//success
			countSuccess(intention);
			return dao;
			} catch(RuntimeException e) {
				countFailure();
				throw e;
			}
		}
	}
	
	private void countSuccess(AccessType intention) {
		if(isPerformanceMonitoringEnabled()) {
			if(intention == AccessType.ReadWrite)
				stats.incrementNewWriteConnections();
			else
				stats.incrementNewReadConnections();
		}
	}

	private void countFailure() {
		if(isPerformanceMonitoringEnabled())
			stats.incrementConnectionFailures();
	}

	/**
	 * Get a SimpleJdbcDaoSupport by primary partition key.
	 * @param partitionDimension The partition dimension
	 * @param primaryIndexKey The partition key
	 * @param intention The permissions with which you wish to acquire the conenction
	 * @return
	 * @throws HiveReadOnlyException
	 */
	public Collection<SimpleJdbcDaoSupport> get(Object primaryIndexKey, final AccessType intention) throws HiveReadOnlyException {
		Collection<NodeSemaphore> semaphores = directory.getNodeSemamphoresOfPrimaryIndexKey(primaryIndexKey);
		Collection<SimpleJdbcDaoSupport> supports = new ArrayList<SimpleJdbcDaoSupport>();
		for(NodeSemaphore semaphore : semaphores)
			supports.add(get(semaphore, intention));
		return supports;
	}

	/**
	 * Get a SimpleJdbcDaoSupport by secondary index key.
	 * @param secondaryIndex The secondary index to search on
	 * @param secondaryIndexKey The secondary key
	 * @param intention The permissions with which you wish to acquire the conenction
	 * @return
	 * @throws HiveReadOnlyException
	 */
	public Collection<SimpleJdbcDaoSupport> get(SecondaryIndex secondaryIndex, Object secondaryIndexKey, final AccessType intention) throws HiveReadOnlyException {
		if(AccessType.ReadWrite == intention) 
			if(Filter.getUnique(hive.getPrimaryIndexKeysOfSecondaryIndexKey(secondaryIndex, secondaryIndexKey)).size() > 1)
				throw new UnsupportedOperationException("Writes for non-unique secondary indexes must be performed using the primary index key.");
		
		Collection<NodeSemaphore> nodeSemaphores = directory.getNodeSemaphoresOfSecondaryIndexKey(secondaryIndex, secondaryIndexKey);
		nodeSemaphores = Filter.getUnique(nodeSemaphores, new Unary<NodeSemaphore, Integer>(){
			public Integer f(NodeSemaphore item) {
				return item.getId();
		}});
		
		Collection<SimpleJdbcDaoSupport> supports = new ArrayList<SimpleJdbcDaoSupport>();
		for(NodeSemaphore semaphore : nodeSemaphores)
			supports.add(get(semaphore, intention));
		return supports;
	}
	private static int hash(Object node, AccessType intention) {
		return HiveUtils.makeHashCode(new Object[] {node, intention});
	}
	
	private static class DataNodeJdbcDaoSupport extends SimpleJdbcDaoSupport
	{
		public DataNodeJdbcDaoSupport(BasicDataSource dataSource)
		{
			this(dataSource, false);
		}
		
		public DataNodeJdbcDaoSupport(BasicDataSource dataSource, Boolean readOnly) {
			dataSource.setDefaultReadOnly(readOnly);
			this.setDataSource(dataSource);
		}
	}

	/**
	 * IMPORTANT -- This bypasses the locking mechanism.  You should only use this
	 * to install schema before data nodes have been populated.
	 */
	public SimpleJdbcDaoSupport getUnsafe(Node node) {
		try {
			NodeSemaphore semaphore = new NodeSemaphore(node.getId(), node.isReadOnly());
			return get(semaphore, AccessType.ReadWrite);
		} catch (HiveException e) {
			throw new RuntimeException(e);
		}
	}

	public String getPartitionDimension() {
		return partitionDimension;
	}
	
	public boolean isPerformanceMonitoringEnabled() {
		return this.stats != null;
	}

	public SimpleJdbcDaoSupport getUnsafe(String nodeName) {
		try {
			Node node = hive.getPartitionDimension(this.getPartitionDimension()).getNodeGroup().getNode(nodeName);
			NodeSemaphore semaphore = new NodeSemaphore(node.getId(), node.isReadOnly());
			return get(semaphore, AccessType.ReadWrite);
		} catch (HiveException e) {
			throw new RuntimeException(e);
		}
	}
}
