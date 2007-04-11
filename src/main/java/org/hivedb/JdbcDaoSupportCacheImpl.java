package org.hivedb;

import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.hivedb.meta.AccessType;
import org.hivedb.meta.Node;
import org.hivedb.meta.NodeSemaphore;
import org.hivedb.meta.SecondaryIndex;
import org.hivedb.meta.persistence.HiveBasicDataSource;
import org.hivedb.util.HiveUtils;
import org.springframework.jdbc.core.simple.SimpleJdbcDaoSupport;

/**
 * @author Britt Crawford (bcrawford@cafepress.com)
 *
 */
public class JdbcDaoSupportCacheImpl implements JdbcDaoSupportCache, Synchronizeable{
	private Hive hive;
	private String partitionDimension;
	private Map<Integer, SimpleJdbcDaoSupport> jdbcDaoSupports;
//	private Map<Integer, NodePerformanceStatistics> stats;
	
	private int revision = Integer.MIN_VALUE;
	
	public JdbcDaoSupportCacheImpl(String partitionDimension, Hive hive) {
		this.partitionDimension = partitionDimension;
		this.hive = hive;
		this.jdbcDaoSupports = new ConcurrentHashMap<Integer, SimpleJdbcDaoSupport>();
//		this.stats = new ConcurrentHashMap<Integer, NodePerformanceStatistics>();
		try {
			sync();
		} catch (HiveException e) {
			// crush for now
		}
	}
	
	/**
	 * Synchronize the cached SimpleJdbcDaoSupports with the state of the hive.  
	 * This method destroys and recreates all SimpleJdbcDaoSupport in the cache.
	 * @throws HiveException
	 */
	public void sync() throws HiveException {
		if(hive.getRevision() != revision) {
			jdbcDaoSupports.clear();
			for(Node node : hive.getPartitionDimension(partitionDimension).getNodeGroup().getNodes()) {
				jdbcDaoSupports.put(hash(node.getId(), AccessType.Read), new DataNodeJdbcDaoSupport(node.getUri(), true));
				if( !hive.isReadOnly() && !node.isReadOnly() )
					addDataSource(node.getId(), AccessType.ReadWrite);
			}
			revision = hive.getRevision();
		}
	}
	
	private SimpleJdbcDaoSupport addDataSource(Integer nodeId, AccessType intention) throws HiveException {
		Node node = hive.getPartitionDimension(partitionDimension).getNodeGroup().getNode(nodeId);
		jdbcDaoSupports.put(hash(nodeId, intention), new DataNodeJdbcDaoSupport(node.getUri()));
		return jdbcDaoSupports.get(hash(nodeId, intention));
	}
	
	private SimpleJdbcDaoSupport get(NodeSemaphore semaphore, AccessType intention) throws HiveReadOnlyException { 
		if(intention == AccessType.ReadWrite && (hive.isReadOnly() || semaphore.isReadOnly())){
			//failure
			throw new HiveReadOnlyException("This partition key cannot be written to at this time.");
		}
		else
			if( jdbcDaoSupports.containsKey(hash(semaphore.getId(), intention))){
				// success case
				return jdbcDaoSupports.get(hash(semaphore.getId(), intention));
			}
			else
				try {
					SimpleJdbcDaoSupport dao = addDataSource(semaphore.getId(), intention);
					//success
					return dao;
				} catch (HiveException e) {
					//failure
					throw new HiveRuntimeException(e.getMessage());
				}
	}
	
	/**
	 * Get a SimpleJdbcDaoSupport by primary partition key.
	 * @param partitionDimension The partition dimension
	 * @param primaryIndexKey The partition key
	 * @param intention The permissions with which you wish to acquire the conenction
	 * @return
	 * @throws HiveReadOnlyException
	 */
	public SimpleJdbcDaoSupport get(Object primaryIndexKey, AccessType intention) throws HiveReadOnlyException {
		try {
			return get(hive.getNodeSemaphoreOfPrimaryIndexKey(partitionDimension, primaryIndexKey), intention);
		} catch (HiveException e) {
			throw new RuntimeException(e);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Get a SimpleJdbcDaoSupport by secondary index key.
	 * @param secondaryIndex The secondary index to search on
	 * @param secondaryIndexKey The secondary key
	 * @param intention The permissions with which you wish to acquire the conenction
	 * @return
	 * @throws HiveReadOnlyException
	 */
	public SimpleJdbcDaoSupport get(SecondaryIndex secondaryIndex, Object secondaryIndexKey, AccessType intention) throws HiveReadOnlyException {
		try {
			return get(hive.getNodeSemaphoreOfSecondaryIndexKey(secondaryIndex, secondaryIndexKey), intention);
		} catch (HiveException e) {
			throw new RuntimeException(e);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
	private static int hash(Object node, AccessType intention) {
		return HiveUtils.makeHashCode(new Object[] {node, intention});
	}
	
	private static class DataNodeJdbcDaoSupport extends SimpleJdbcDaoSupport
	{
		public DataNodeJdbcDaoSupport(String databaseUri)
		{
			this.setDataSource(new HiveBasicDataSource(databaseUri));
		}
		
		public DataNodeJdbcDaoSupport(String databaseUri, boolean readOnly)
		{
			HiveBasicDataSource ds = new HiveBasicDataSource(databaseUri);
			ds.setDefaultReadOnly(readOnly);
			this.setDataSource(ds);
		}
	}

	/**
	 * IMPORTANT -- This bypasses the locking mechanism.  You should only use this
	 * to install schema before data nodes have been populated.
	 */
	public SimpleJdbcDaoSupport getUnsafe(Node node) throws HiveReadOnlyException {
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
}
