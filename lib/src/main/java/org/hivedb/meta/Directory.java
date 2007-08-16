package org.hivedb.meta;

import static org.hivedb.management.statistics.DirectoryPerformanceStatisticsMBean.PRIMARY_INDEX_DELETE;
import static org.hivedb.management.statistics.DirectoryPerformanceStatisticsMBean.PRIMARY_INDEX_READ;
import static org.hivedb.management.statistics.DirectoryPerformanceStatisticsMBean.PRIMARY_INDEX_WRITE;
import static org.hivedb.management.statistics.DirectoryPerformanceStatisticsMBean.SECONDARY_INDEX_DELETE;
import static org.hivedb.management.statistics.DirectoryPerformanceStatisticsMBean.SECONDARY_INDEX_READ;
import static org.hivedb.management.statistics.DirectoryPerformanceStatisticsMBean.SECONDARY_INDEX_WRITE;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collection;
import java.util.Date;

import javax.sql.DataSource;

import org.hivedb.DirectoryCorruptionException;
import org.hivedb.HiveKeyNotFoundException;
import org.hivedb.StatisticsProxy;
import org.hivedb.management.statistics.Counter;
import org.hivedb.management.statistics.NoOpStatistics;
import org.hivedb.meta.persistence.HiveBasicDataSource;
import org.hivedb.util.Delay;
import org.hivedb.util.JdbcTypeMapper;
import org.hivedb.util.Proxies;
import org.hivedb.util.QuickCache;
import org.hivedb.util.database.RowMappers;
import org.hivedb.util.database.Statements;
import org.hivedb.util.functional.Atom;
import org.hivedb.util.functional.Transform;
import org.hivedb.util.functional.Unary;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.PreparedStatementCreatorFactory;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.ParameterizedRowMapper;
import org.springframework.jdbc.core.simple.SimpleJdbcDaoSupport;

public class Directory extends SimpleJdbcDaoSupport implements NodeResolver {
	private PartitionDimension partitionDimension;
	private Counter performanceStatistics;
	private boolean performanceMonitoringEnabled = false;
	private IndexSqlFormatter sql = new IndexSqlFormatter();
	
	public Directory(PartitionDimension dimension, DataSource dataSource, Counter performanceStatistics) {
		this(dimension, dataSource);
		this.setPerformanceStatistics(performanceStatistics);
		this.setPerformanceMonitoringEnabled(true);
	}
	
	public Directory(PartitionDimension dimension, DataSource dataSource) {
		this.partitionDimension = dimension;
		this.setDataSource(dataSource);
		this.performanceStatistics = new NoOpStatistics();
	}
	
	public Directory(PartitionDimension dimension) {
		this.partitionDimension = dimension;
		this.setDataSource(new HiveBasicDataSource(dimension.getIndexUri()));
		this.performanceStatistics = new NoOpStatistics();
	}
	
	/* (non-Javadoc)
	 * @see org.hivedb.meta.HiveDirectory#getPartitionDimension()
	 */
	public PartitionDimension getPartitionDimension() {
		return this.partitionDimension;
	}
	
	/* (non-Javadoc)
	 * @see org.hivedb.meta.HiveDirectory#insertPrimaryIndexKey(org.hivedb.meta.Node, java.lang.Object)
	 */
	public void insertPrimaryIndexKey(Node node, Object primaryIndexKey) {
		int[] types = new int[]{JdbcTypeMapper.primitiveTypeToJdbcType(primaryIndexKey.getClass()), Types.INTEGER, Types.DATE};
		Object[] parameters = new Object[] {primaryIndexKey,node.getId(),new Date(System.currentTimeMillis()) };
		doUpdate(sql.insertPrimaryIndexKey(partitionDimension), types, parameters, PRIMARY_INDEX_WRITE);
	}

	
	/* (non-Javadoc)
	 * @see org.hivedb.meta.HiveDirectory#insertSecondaryIndexKey(org.hivedb.meta.SecondaryIndex, java.lang.Object, java.lang.Object)
	 */
	public void insertSecondaryIndexKey(SecondaryIndex secondaryIndex, Object secondaryIndexKey, Object primaryindexKey) {
		Object[] parameters = new Object[] {secondaryIndexKey,primaryindexKey};
		int[] types = new int[]{secondaryIndex.getColumnInfo().getColumnType(),secondaryIndex.getResource().getColumnType()};
		doUpdate(sql.insertSecondaryIndexKey(secondaryIndex), types, parameters, SECONDARY_INDEX_WRITE);
	}
	
	private static QuickCache cache = new QuickCache();

	/* (non-Javadoc)
	 * @see org.hivedb.meta.HiveDirectory#updatePrimaryIndexKeyReadOnly(java.lang.Object, boolean)
	 */
	public void updatePrimaryIndexKeyReadOnly(Object primaryIndexKey, boolean isReadOnly) {
		Object[] parameters = new Object[] {isReadOnly,primaryIndexKey};
		int[] types = new int[]{Types.BOOLEAN, JdbcTypeMapper.primitiveTypeToJdbcType(primaryIndexKey.getClass())};
		doUpdate(sql.updateReadOnlyOfPrimaryIndexKey(partitionDimension), types, parameters, PRIMARY_INDEX_WRITE);
	}
	
	/* (non-Javadoc)
	 * @see org.hivedb.meta.HiveDirectory#updatePrimaryIndexOfSecondaryKey(org.hivedb.meta.SecondaryIndex, java.lang.Object, java.lang.Object, java.lang.Object)
	 */
	public void updatePrimaryIndexKeyOfResourceId(Resource resource, Object resourceId, Object originalPrimaryIndexKey, Object newPrimaryIndexKey) {
		Object[] parameters = new Object[] {
			newPrimaryIndexKey,
			resourceId,
			originalPrimaryIndexKey
		};
		int[] types = new int[]{
			JdbcTypeMapper.primitiveTypeToJdbcType(newPrimaryIndexKey.getClass()),
			resource.getColumnType(),
			JdbcTypeMapper.primitiveTypeToJdbcType(originalPrimaryIndexKey.getClass())
		};
		doUpdate(
			sql.updateSecondaryIndexKey(resource.getIdIndex()),
			types,
			parameters,
			SECONDARY_INDEX_WRITE);
	}
	
	/* (non-Javadoc)
	 * @see org.hivedb.meta.HiveDirectory#deletePrimaryIndexKey(java.lang.Object)
	 */
	public void deletePrimaryIndexKey(Object primaryIndexKey) {
		doUpdate(
			sql.deletePrimaryIndexKey(partitionDimension), 
			new int[]{JdbcTypeMapper.primitiveTypeToJdbcType(primaryIndexKey.getClass())}, 
			new Object[] {primaryIndexKey}, 
			PRIMARY_INDEX_DELETE);
	}

	/* (non-Javadoc)
	 * @see org.hivedb.meta.HiveDirectory#deleteSecondaryIndexKey(org.hivedb.meta.SecondaryIndex, java.lang.Object)
	 */
	public void deleteSecondaryIndexKey(SecondaryIndex secondaryIndex, Object secondaryIndexKey, Object resourceId) {
		Object[] parameters = new Object[] {
			secondaryIndexKey,
			resourceId
		};
		int[] types = new int[] {
			secondaryIndex.getColumnInfo().getColumnType(),
			JdbcTypeMapper.primitiveTypeToJdbcType(resourceId.getClass())	
		};
		doUpdate(sql.deleteSingleSecondaryIndexKey(secondaryIndex), types, parameters, SECONDARY_INDEX_DELETE);
	}
	
	/* (non-Javadoc)
	 * @see org.hivedb.meta.HiveDirectory#doesPrimaryIndexKeyExist(java.lang.Object)
	 */
	public boolean doesPrimaryIndexKeyExist(Object primaryIndexKey) {
		Collection count = doRead(sql.checkExistenceOfPrimaryKey(partitionDimension), 
				new Object[] { primaryIndexKey }, 
				RowMappers.newTrueRowMapper(),
				PRIMARY_INDEX_READ);
		return count.size() > 0;
	}
	
	/* (non-Javadoc)
	 * @see org.hivedb.meta.HiveDirectory#getNodeIdsOfPrimaryIndexKey(java.lang.Object)
	 */
	public Collection<Integer> getNodeIdsOfPrimaryIndexKey(Object primaryIndexKey) {
		return Transform.map(semaphoreToId(), getNodeSemamphoresOfPrimaryIndexKey(primaryIndexKey));
	}
	
	/* (non-Javadoc)
	 * @see org.hivedb.meta.HiveDirectory#getNodeSemamphoresOfPrimaryIndexKey(java.lang.Object)
	 */
	public Collection<NodeSemaphore> getNodeSemamphoresOfPrimaryIndexKey(Object primaryIndexKey) {
		return doRead(sql.selectNodeSemaphoreOfPrimaryIndexKey(partitionDimension), 
				new Object[] { primaryIndexKey }, 
				new NodeSemaphoreRowMapper(),
				PRIMARY_INDEX_READ);
	}
	
	/* (non-Javadoc)
	 * @see org.hivedb.meta.HiveDirectory#getReadOnlyOfPrimaryIndexKey(java.lang.Object)
	 */
	@SuppressWarnings("unchecked")
	public boolean getReadOnlyOfPrimaryIndexKey(Object primaryIndexKey) {
		Boolean readOnly = false;
		for(Boolean b : Transform.map(semaphoreToReadOnly(), getNodeSemamphoresOfPrimaryIndexKey(primaryIndexKey)))
			readOnly |= b;
		return readOnly;
	}
	
	/* (non-Javadoc)
	 * @see org.hivedb.meta.HiveDirectory#doesSecondaryIndexKeyExist(org.hivedb.meta.SecondaryIndex, java.lang.Object)
	 */
	public boolean doesSecondaryIndexKeyExist(SecondaryIndex secondaryIndex,Object secondaryIndexKey) {
		Collection<Object> count = 
				doRead(
					sql.checkExistenceOfSecondaryIndexSql(secondaryIndex), 
					new Object[] { secondaryIndexKey }, 
					RowMappers.newTrueRowMapper(),
					SECONDARY_INDEX_READ);
		return count.size() > 0;
	}
	
	/* (non-Javadoc)
	 * @see org.hivedb.meta.HiveDirectory#getNodeIdsOfSecondaryIndexKey(org.hivedb.meta.SecondaryIndex, java.lang.Object)
	 */
	@SuppressWarnings("unchecked")
	public Collection<Integer> getNodeIdsOfSecondaryIndexKey(SecondaryIndex secondaryIndex, Object secondaryIndexKey)
	{
		return Transform.map(semaphoreToId(), getNodeSemaphoresOfSecondaryIndexKey(secondaryIndex, secondaryIndexKey));
	}
	
	/* (non-Javadoc)
	 * @see org.hivedb.meta.HiveDirectory#getNodeSemaphoresOfSecondaryIndexKey(org.hivedb.meta.SecondaryIndex, java.lang.Object)
	 */
	public Collection<NodeSemaphore> getNodeSemaphoresOfSecondaryIndexKey(SecondaryIndex secondaryIndex, Object secondaryIndexKey)
	{
		return doRead(
			sql.selectNodeSemaphoresOfSecondaryIndexKey(secondaryIndex), 
			new Object[] {secondaryIndexKey}, 
			new NodeSemaphoreRowMapper(), 
			SECONDARY_INDEX_READ);
	}
	
	/* (non-Javadoc)
	 * @see org.hivedb.meta.HiveDirectory#getPrimaryIndexKeysOfSecondaryIndexKey(org.hivedb.meta.SecondaryIndex, java.lang.Object)
	 */
	@SuppressWarnings("unchecked")
	public Collection<Object> getPrimaryIndexKeysOfSecondaryIndexKey(SecondaryIndex secondaryIndex, Object secondaryIndexKey)
	{
		return doRead(
			sql.selectPrimaryIndexKeysOfSecondaryIndexKey(secondaryIndex), 
			new Object[] {secondaryIndexKey}, 
			RowMappers.newObjectRowMapper(secondaryIndex.getResource().getPartitionDimension().getColumnType()),
			SECONDARY_INDEX_READ);
	}
	
	/* (non-Javadoc)
	 * @see org.hivedb.meta.HiveDirectory#getSecondaryIndexKeysOfPrimaryIndexKey(org.hivedb.meta.SecondaryIndex, java.lang.Object)
	 */
	@SuppressWarnings("unchecked")
	public Collection<Object> getSecondaryIndexKeysOfPrimaryIndexKey(SecondaryIndex secondaryIndex, Object primaryIndexKey)
	{
		return doRead(
			sql.selectSecondaryIndexKeysOfPrimaryKey(secondaryIndex),
			new Object[] { primaryIndexKey }, 
			RowMappers.newObjectRowMapper(secondaryIndex.getColumnInfo().getColumnType()),
			SECONDARY_INDEX_READ);
	}
	
	public Counter getPerformanceStatistics() {
		return performanceStatistics;
	}

	public void setPerformanceStatistics(
			Counter performanceStatistics) {
		this.performanceStatistics = performanceStatistics;
	}

	public boolean isPerformanceMonitoringEnabled() {
		return performanceStatistics.getClass() != NoOpStatistics.class && performanceMonitoringEnabled;
	}

	public void setPerformanceMonitoringEnabled(boolean performanceMonitoringEnabled) {
		this.performanceMonitoringEnabled = performanceMonitoringEnabled;
	}

	public void deleteResourceId(Resource resource, Object id) {
		doUpdate(sql.deleteResourceId(resource), new int[] {resource.getColumnType()}, new Object[] {id}, SECONDARY_INDEX_DELETE);
	}

	public boolean doesResourceIdExist(Resource resource, Object id) {
		return doesSecondaryIndexKeyExist(resource.getIdIndex(), id);
	}

	public Collection getResourceIdsOfSecondaryIndexKey(SecondaryIndex secondaryIndex, Object secondaryIndexKey) {
		return doRead(
				sql.selectResourceIdsOfSecondaryIndexKey(secondaryIndex),
				new Object[] { secondaryIndexKey }, 
				RowMappers.newObjectRowMapper(secondaryIndex.getColumnInfo().getColumnType()),
				SECONDARY_INDEX_READ);
	}
	
	public Collection getResourceIdsOfPrimaryIndexKey(Resource resource, Object primaryIndexKey) {
		return doRead(
				sql.selectResourceIdsOfPrimaryIndexKey(resource.getIdIndex()),
				new Object[] { primaryIndexKey }, 
				RowMappers.newObjectRowMapper(resource.getColumnType()),
				SECONDARY_INDEX_READ);
	}
	
	public class NodeSemaphoreRowMapper implements ParameterizedRowMapper {
		public Object mapRow(ResultSet rs, int arg1) throws SQLException {
			return new NodeSemaphore(rs.getInt("node"), rs.getBoolean("read_only"));
		}	
	}

	public Collection getSecondaryIndexKeysOfResourceId(SecondaryIndex secondaryIndex, Object id) {
		return doRead(
				sql.selectSecondaryIndexKeyOfResourceId(secondaryIndex), 
				new Object[] { id }, 
				RowMappers.newObjectRowMapper(secondaryIndex.getColumnInfo().getColumnType()), 
				SECONDARY_INDEX_READ);
	}
	
	@SuppressWarnings("unchecked")
	private<T> Collection<T> doRead(String sql, Object[] parameters, RowMapper mapper, String statsKey) {
		StatisticsProxy<Collection<Object>> proxy = 
			Proxies.newJdbcSqlQueryProxy(performanceStatistics, 
					statsKey,
					sql, 
					parameters, 
					mapper,
					getJdbcTemplate());
		
		try{
			return (Collection<T>) proxy.execute();
		} catch(EmptyResultDataAccessException e) {
			throw new HiveKeyNotFoundException(String.format("Unable to get secondary index keys of primary index key %s, key not found.", parameters[0]), parameters[0],e);
		}
	}
	
	private void doUpdate(String sql, int[] types, Object[] parameters, String statsKey){
		PreparedStatementCreatorFactory factory = 
			Statements.newStmtCreatorFactory(sql, types);
		Proxies.newJdbcUpdateProxy(performanceStatistics, statsKey, parameters, factory, getJdbcTemplate()).execute();
	}

	public void insertResourceId(Resource resource, Object id, Object primaryIndexKey) {
		doUpdate(
				sql.insertSecondaryIndexKey(resource.getIdIndex()), 
				new int[] {resource.getColumnType(),resource.getPartitionDimension().getColumnType()}, 
				new Object[]{id,primaryIndexKey}, 
				SECONDARY_INDEX_WRITE);
	}

	@SuppressWarnings("unchecked")
	public Object getPrimaryIndexKeyOfResourceId(Resource resource, Object id) {
		Collection keys = getPrimaryIndexKeysOfSecondaryIndexKey(resource.getIdIndex(), id);
		if( keys.size() == 0)
			throw new HiveKeyNotFoundException(String.format("Unable to find resource %s with id %s", resource.getName(), id), id);
		else if(keys.size() > 1)
			throw new DirectoryCorruptionException(String.format("Directory corruption: Resource %s with id %s is owned more than one primary key.", resource.getName(), id));
		return Atom.getFirstOrNull(keys);
	}

	public boolean getReadOnlyOfResourceId(Resource resource, Object id) {
		Collection<NodeSemaphore> semaphores = getNodeSemaphoresOfSecondaryIndexKey(resource.getIdIndex(), id);
		if( semaphores.size() == 0)
			throw new HiveKeyNotFoundException(String.format("Unable to find resource %s with id %s", resource.getName(), id), id);
		boolean readOnly = false;
		for(NodeSemaphore s : semaphores)
			readOnly |= s.isReadOnly();
		return readOnly;
	}
	
	public Collection<Integer> getNodeIdsOfResourceId(Resource resource, Object id) {
		return Transform.map(semaphoreToId(), getNodeSemaphoresOfSecondaryIndexKey(resource.getIdIndex(), id));
	}
	
	public Collection<NodeSemaphore> getNodeSemaphoresOfResourceId(Resource resource, Object id) {
		return getNodeSemaphoresOfSecondaryIndexKey(resource.getIdIndex(), id);
	}
	
	public void updateResourceIdOfSecondaryIndexKey(SecondaryIndex secondaryIndex, Object secondaryIndexKey, 
		Object originalResourceId, Object newResourceId) {
		Object[] parameters = new Object[] {
				newResourceId,
				secondaryIndexKey,
				originalResourceId
			};
			int[] types = new int[]{
				secondaryIndex.getResource().getColumnType(),
				secondaryIndex.getColumnInfo().getColumnType(),
				secondaryIndex.getResource().getColumnType()
			};
			doUpdate(
				sql.updateSecondaryIndexKey(secondaryIndex),
				types,
				parameters,
				SECONDARY_INDEX_WRITE);
	}
	
	public Unary<NodeSemaphore,Integer> semaphoreToId() {
		return new Unary<NodeSemaphore, Integer>(){

			public Integer f(NodeSemaphore item) {
				return item.getId();
			}};
	}
	
	public Unary<NodeSemaphore, Boolean> semaphoreToReadOnly() {
		return new Unary<NodeSemaphore, Boolean>(){

			public Boolean f(NodeSemaphore item) {
				return item.isReadOnly();
			}};
	}
	
	public BatchIndexWriter batch() {
		final Directory d = this;
		return cache.get(BatchIndexWriter.class, new Delay<BatchIndexWriter>() {
			public BatchIndexWriter f() {
				return new BatchIndexWriter(d);
			}	
		});
	}
}
