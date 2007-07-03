package org.hivedb.meta;

import static org.hivedb.management.statistics.DirectoryPerformanceStatisticsMBean.PRIMARY_INDEX_DELETE;
import static org.hivedb.management.statistics.DirectoryPerformanceStatisticsMBean.PRIMARY_INDEX_READ;
import static org.hivedb.management.statistics.DirectoryPerformanceStatisticsMBean.PRIMARY_INDEX_WRITE;
import static org.hivedb.management.statistics.DirectoryPerformanceStatisticsMBean.SECONDARY_INDEX_DELETE;
import static org.hivedb.management.statistics.DirectoryPerformanceStatisticsMBean.SECONDARY_INDEX_READ;
import static org.hivedb.management.statistics.DirectoryPerformanceStatisticsMBean.SECONDARY_INDEX_WRITE;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collection;

import javax.sql.DataSource;

import org.hivedb.HiveKeyNotFoundException;
import org.hivedb.HiveRuntimeException;
import org.hivedb.StatisticsProxy;
import org.hivedb.management.statistics.Counter;
import org.hivedb.management.statistics.NoOpStatistics;
import org.hivedb.meta.persistence.HiveBasicDataSource;
import org.hivedb.util.JdbcTypeMapper;
import org.hivedb.util.Proxies;
import org.hivedb.util.database.RowMappers;
import org.hivedb.util.database.Statements;
import org.hivedb.util.functional.Filter;
import org.hivedb.util.functional.Transform;
import org.hivedb.util.functional.Unary;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreatorFactory;
import org.springframework.jdbc.core.simple.ParameterizedRowMapper;
import org.springframework.jdbc.core.simple.SimpleJdbcDaoSupport;

public class Directory extends SimpleJdbcDaoSupport implements NodeResolver {
	private PartitionDimension partitionDimension;
	private Counter performanceStatistics;
	private boolean performanceMonitoringEnabled = false;
	
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
	public void insertPrimaryIndexKey(final Collection<Node> nodes, final Object primaryIndexKey) {
		final JdbcTemplate j = getJdbcTemplate();
		final PreparedStatementCreatorFactory insertFactory = 
			Statements.newStmtCreatorFactory(
					insertPrimaryIndexSql(partitionDimension),
					JdbcTypeMapper.primitiveTypeToJdbcType(primaryIndexKey.getClass()),
					Types.INTEGER, Types.DATE);
		
		try {
			for(Node node : nodes) {
				final Object[] parameters = new Object[] {primaryIndexKey,node.getId(),new Date(System.currentTimeMillis()) };
				StatisticsProxy<Integer> proxy = 
					Proxies.newJdbcUpdateProxy(performanceStatistics,PRIMARY_INDEX_WRITE,parameters,insertFactory,j);
				if(proxy.execute() == 0)
					throw new HiveRuntimeException(String.format("Unable to insert primary key %s into node %s", primaryIndexKey, node.getId()));
			}
		} catch(Exception e) {
			deletePrimaryIndexKey(primaryIndexKey);
			throw new HiveRuntimeException(
					String.format("Error while inserting primary key %s into nodes %s. Caused by: %s", 
							primaryIndexKey, getIds(nodes), e.getMessage()), e);
		}
	}

	
	/* (non-Javadoc)
	 * @see org.hivedb.meta.HiveDirectory#insertSecondaryIndexKey(org.hivedb.meta.SecondaryIndex, java.lang.Object, java.lang.Object)
	 */
	public void insertSecondaryIndexKey(final SecondaryIndex secondaryIndex, final Object secondaryIndexKey, Object primaryindexKey) {
		final Object[] parameters = new Object[] {
			secondaryIndexKey,
			primaryindexKey
		};
		final PreparedStatementCreatorFactory insertFactory = 
			Statements.newStmtCreatorFactory(insertSecondaryIndexKeySql(secondaryIndex, partitionDimension), 
				JdbcTypeMapper.primitiveTypeToJdbcType(secondaryIndexKey.getClass()),
				JdbcTypeMapper.primitiveTypeToJdbcType(primaryindexKey.getClass()));
		Proxies.newJdbcUpdateProxy(performanceStatistics, SECONDARY_INDEX_WRITE, parameters, insertFactory, getJdbcTemplate()).execute();
	}
	
	/* (non-Javadoc)
	 * @see org.hivedb.meta.HiveDirectory#updatePrimaryIndexKey(org.hivedb.meta.Node, java.lang.Object)
	 */
	public void updatePrimaryIndexKey(final Collection<Node> nodes, final Object primaryIndexKey) {
		deletePrimaryIndexKey(primaryIndexKey);
		insertPrimaryIndexKey(nodes, primaryIndexKey);
	}
	
	/* (non-Javadoc)
	 * @see org.hivedb.meta.HiveDirectory#updatePrimaryIndexKeyReadOnly(java.lang.Object, boolean)
	 */
	public void updatePrimaryIndexKeyReadOnly(final Object primaryIndexKey, boolean isReadOnly) {
		final Object[] parameters = new Object[] {
				isReadOnly,
				primaryIndexKey 
		};
		PreparedStatementCreatorFactory updateFactory = 
			Statements.newStmtCreatorFactory("update " + IndexSchema.getPrimaryIndexTableName(partitionDimension) + " set read_only = ? where id = ?", 
					Types.BOOLEAN, JdbcTypeMapper.primitiveTypeToJdbcType(primaryIndexKey.getClass()));
		StatisticsProxy<Integer> proxy = Proxies.newJdbcUpdateProxy(performanceStatistics, PRIMARY_INDEX_WRITE, parameters, updateFactory, getJdbcTemplate());
		if(proxy.execute() == 0)
			throw new HiveKeyNotFoundException(String.format("Unable to update primary index key %s read-only, key not found.", primaryIndexKey), primaryIndexKey);
	}
	
	/* (non-Javadoc)
	 * @see org.hivedb.meta.HiveDirectory#updatePrimaryIndexOfSecondaryKey(org.hivedb.meta.SecondaryIndex, java.lang.Object, java.lang.Object, java.lang.Object)
	 */
	public void updatePrimaryIndexOfSecondaryKey(final SecondaryIndex secondaryIndex, final Object secondaryIndexKey, final Object originalPrimaryIndexKey, final Object newPrimaryIndexKey) {
		final Object[] parameters = new Object[] {
			newPrimaryIndexKey,
			secondaryIndexKey,
			originalPrimaryIndexKey
		};
		PreparedStatementCreatorFactory updateFactory = 
			Statements.newStmtCreatorFactory(updateSecondaryIndexSql(secondaryIndex),
					JdbcTypeMapper.primitiveTypeToJdbcType(originalPrimaryIndexKey.getClass()),
					JdbcTypeMapper.primitiveTypeToJdbcType(secondaryIndexKey.getClass()),
					JdbcTypeMapper.primitiveTypeToJdbcType(newPrimaryIndexKey.getClass()));
		StatisticsProxy<Integer> proxy = Proxies.newJdbcUpdateProxy(performanceStatistics, SECONDARY_INDEX_WRITE, parameters, updateFactory, getJdbcTemplate());
		if(proxy.execute() == 0)
			throw new HiveKeyNotFoundException(String.format("Unable to update Secondary index key %s key not found.", secondaryIndexKey), originalPrimaryIndexKey);
	}
	
	/* (non-Javadoc)
	 * @see org.hivedb.meta.HiveDirectory#deleteAllSecondaryIndexKeysOfPrimaryIndexKey(org.hivedb.meta.SecondaryIndex, java.lang.Object)
	 */
	public void deleteAllSecondaryIndexKeysOfPrimaryIndexKey(SecondaryIndex secondaryIndex, Object primaryIndexKey) {
		PreparedStatementCreatorFactory deleteFactory = 
			Statements.newStmtCreatorFactory(deleteSecondaryIndexKeysSql(secondaryIndex), JdbcTypeMapper.primitiveTypeToJdbcType(primaryIndexKey.getClass()));
		Proxies.newJdbcUpdateProxy(performanceStatistics, SECONDARY_INDEX_WRITE, new Object[] {primaryIndexKey}, deleteFactory, getJdbcTemplate()).execute();
	}
	
	/* (non-Javadoc)
	 * @see org.hivedb.meta.HiveDirectory#deletePrimaryIndexKey(java.lang.Object)
	 */
	public void deletePrimaryIndexKey(final Object primaryIndexKey) {
		final Object[] parameters = new Object[] {primaryIndexKey};
		final PreparedStatementCreatorFactory deleteFactory = 
			Statements.newStmtCreatorFactory(deletePrimaryIndexSql(partitionDimension), JdbcTypeMapper.primitiveTypeToJdbcType(primaryIndexKey.getClass()));
		StatisticsProxy<Integer> proxy = Proxies.newJdbcUpdateProxy(performanceStatistics, PRIMARY_INDEX_DELETE, parameters, deleteFactory, getJdbcTemplate());
		proxy.execute();
	}

	/* (non-Javadoc)
	 * @see org.hivedb.meta.HiveDirectory#deleteSecondaryIndexKey(org.hivedb.meta.SecondaryIndex, java.lang.Object)
	 */
	public void deleteSecondaryIndexKey(final SecondaryIndex secondaryIndex, final Object secondaryIndexKey, final Object primaryIndexKey) {
		final Object[] parameters = new Object[] {
			secondaryIndexKey,
			primaryIndexKey
		};
		PreparedStatementCreatorFactory deleteFactory = 
			Statements.newStmtCreatorFactory(deleteSingleSecondaryIndexKey(secondaryIndex), 
					JdbcTypeMapper.primitiveTypeToJdbcType(primaryIndexKey.getClass()), 
					JdbcTypeMapper.primitiveTypeToJdbcType(secondaryIndexKey.getClass()));
		Proxies.newJdbcUpdateProxy(performanceStatistics, SECONDARY_INDEX_DELETE, parameters, deleteFactory, getJdbcTemplate()).execute();
	}
	
	/* (non-Javadoc)
	 * @see org.hivedb.meta.HiveDirectory#doesPrimaryIndexKeyExist(java.lang.Object)
	 */
	public boolean doesPrimaryIndexKeyExist(final Object primaryIndexKey) {
		StatisticsProxy<Collection<Integer>> proxy = 
			Proxies.newJdbcSqlQueryProxy(
					performanceStatistics, 
					PRIMARY_INDEX_READ, 
					checkExistenceOfPrimaryKeySql(), 
					new Object[] { primaryIndexKey }, 
					RowMappers.newTrueRowMapper(), 
					getJdbcTemplate());
		try {
			return proxy.execute().size() > 0;
		} catch( EmptyResultDataAccessException e) {
			return false;
		}
	}
	
	/* (non-Javadoc)
	 * @see org.hivedb.meta.HiveDirectory#getNodeIdsOfPrimaryIndexKey(java.lang.Object)
	 */
	public Collection<Integer> getNodeIdsOfPrimaryIndexKey(final Object primaryIndexKey) {
		StatisticsProxy<Collection<Integer>> proxy = 
			Proxies.newJdbcSqlQueryProxy(
					performanceStatistics, 
					PRIMARY_INDEX_READ, 
					selectNodeIdsByPrimaryIndexSql(), 
					new Object[] { primaryIndexKey }, 
					RowMappers.newIntegerRowMapper(), 
					getJdbcTemplate());
		try{
			return proxy.execute();
		} catch(EmptyResultDataAccessException e) {
			throw new HiveKeyNotFoundException(String.format("Unable to get node of primary index key %s, key not found.", primaryIndexKey), primaryIndexKey,e);
		}
	}
	
	/* (non-Javadoc)
	 * @see org.hivedb.meta.HiveDirectory#getNodeSemamphoresOfPrimaryIndexKey(java.lang.Object)
	 */
	public Collection<NodeSemaphore> getNodeSemamphoresOfPrimaryIndexKey(final Object primaryIndexKey) {
		StatisticsProxy<Collection<NodeSemaphore>> proxy = Proxies.newJdbcSqlQueryProxy(
				performanceStatistics, 
				PRIMARY_INDEX_READ, 
				selectNodeSemaphoreOfPrimaryIndexKeySql(partitionDimension), 
				new Object[] { primaryIndexKey }, 
				new NodeSemaphoreRowMapper(), 
				getJdbcTemplate());
		try{
			return proxy.execute();
		} catch(EmptyResultDataAccessException e) {
			throw new HiveKeyNotFoundException(String.format("Unable to get nodeSemaphore of primary index key %s, key not found.", primaryIndexKey), primaryIndexKey,e);
		}
	}
	
	/* (non-Javadoc)
	 * @see org.hivedb.meta.HiveDirectory#getReadOnlyOfPrimaryIndexKey(java.lang.Object)
	 */
	@SuppressWarnings("unchecked")
	public boolean getReadOnlyOfPrimaryIndexKey(final Object primaryIndexKey) {
		StatisticsProxy<Collection<Boolean>> proxy = Proxies.newJdbcSqlQueryProxy(performanceStatistics, PRIMARY_INDEX_READ, getReadOnlyOfPrimaryndexKeySql(partitionDimension), new Object[] { primaryIndexKey }, RowMappers.newBooleanRowMapper(), getJdbcTemplate());
		try{
			Boolean readOnly = false;
			for(Boolean b : proxy.execute())
				readOnly |= b;
			return readOnly;
		} catch(EmptyResultDataAccessException e) {
			throw new HiveKeyNotFoundException(String.format("Unable to get read-only of primary index key %s, key not found.", primaryIndexKey), primaryIndexKey,e);
		}
	}
	
	/* (non-Javadoc)
	 * @see org.hivedb.meta.HiveDirectory#doesSecondaryIndexKeyExist(org.hivedb.meta.SecondaryIndex, java.lang.Object)
	 */
	public boolean doesSecondaryIndexKeyExist(final SecondaryIndex secondaryIndex,final Object secondaryIndexKey) {
		StatisticsProxy<Collection<Integer>> proxy = 
			Proxies.newJdbcSqlQueryProxy(
					performanceStatistics, 
					SECONDARY_INDEX_READ, 
					checkExistenceOfSecondaryIndexSql(secondaryIndex), 
					new Object[] { secondaryIndexKey }, 
					RowMappers.newTrueRowMapper(), 
					getJdbcTemplate());
		try {
			return proxy.execute().size() > 0;
		} catch( EmptyResultDataAccessException e) {
			return false;
		}
	}
	
	/* (non-Javadoc)
	 * @see org.hivedb.meta.HiveDirectory#getNodeIdsOfSecondaryIndexKey(org.hivedb.meta.SecondaryIndex, java.lang.Object)
	 */
	@SuppressWarnings("unchecked")
	public Collection<Integer> getNodeIdsOfSecondaryIndexKey(final SecondaryIndex secondaryIndex, final Object secondaryIndexKey)
	{
		StatisticsProxy<Collection<Integer>> proxy = 
			Proxies.newJdbcSqlQueryProxy(performanceStatistics, 
					SECONDARY_INDEX_READ, 
					getNodeIdsOfSecondaryIndexKeySql(secondaryIndex), 
					new Object[] { secondaryIndexKey }, 
					RowMappers.newIntegerRowMapper(), 
					getJdbcTemplate());
		try{
			return Filter.getUnique(proxy.execute());
		} catch(EmptyResultDataAccessException e) {
			throw new HiveKeyNotFoundException(String.format("Unable to get node id of secondary index key %s, key not found.", secondaryIndexKey), secondaryIndexKey,e);
		}
	}
	
	/* (non-Javadoc)
	 * @see org.hivedb.meta.HiveDirectory#getNodeSemaphoresOfSecondaryIndexKey(org.hivedb.meta.SecondaryIndex, java.lang.Object)
	 */
	public Collection<NodeSemaphore> getNodeSemaphoresOfSecondaryIndexKey(final SecondaryIndex secondaryIndex, final Object secondaryIndexKey)
	{
		StatisticsProxy<Collection<NodeSemaphore>> proxy = 
			Proxies.newJdbcSqlQueryProxy(performanceStatistics, 
					SECONDARY_INDEX_READ, 
					getNodeSemaphoreOfSecondaryIndexKeySql(secondaryIndex), 
					new Object[] {secondaryIndexKey}, 
					new NodeSemaphoreRowMapper(), 
					getJdbcTemplate());
		
		try{
			return proxy.execute();
		} catch(EmptyResultDataAccessException e) {
			throw new HiveKeyNotFoundException(String.format("Unable to get nodeSemaphore of secondayr index key %s, key not found.", secondaryIndexKey), secondaryIndexKey,e);
		}
	}
	
	/* (non-Javadoc)
	 * @see org.hivedb.meta.HiveDirectory#getPrimaryIndexKeysOfSecondaryIndexKey(org.hivedb.meta.SecondaryIndex, java.lang.Object)
	 */
	@SuppressWarnings("unchecked")
	public Collection<Object> getPrimaryIndexKeysOfSecondaryIndexKey(final SecondaryIndex secondaryIndex, final Object secondaryIndexKey)
	{
		StatisticsProxy<Collection<Object>> proxy = 
			Proxies.newJdbcSqlQueryProxy(performanceStatistics, 
					SECONDARY_INDEX_READ, 
					getPrimaryIndexKeysOfSecondaryIndexKeySql(secondaryIndex), 
					new Object[] {secondaryIndexKey}, 
					RowMappers.newObjectRowMapper(secondaryIndex.getResource().getPartitionDimension().getColumnType()), 
					getJdbcTemplate());
		
		try{
			return proxy.execute();
		} catch(EmptyResultDataAccessException e) {
			throw new HiveKeyNotFoundException(String.format("Unable to get primary Index Key of secondary index key %s, key not found.", secondaryIndexKey), secondaryIndexKey,e);
		}
	}
	
	/* (non-Javadoc)
	 * @see org.hivedb.meta.HiveDirectory#getSecondaryIndexKeysOfPrimaryIndexKey(org.hivedb.meta.SecondaryIndex, java.lang.Object)
	 */
	@SuppressWarnings("unchecked")
	public Collection<Object> getSecondaryIndexKeysOfPrimaryIndexKey(final SecondaryIndex secondaryIndex, final Object primaryIndexKey)
	{
		final String secondaryIndexTableName = IndexSchema.getSecondaryIndexTableName(partitionDimension, secondaryIndex);
		StatisticsProxy<Collection<Object>> proxy = 
			Proxies.newJdbcSqlQueryProxy(performanceStatistics, 
					SECONDARY_INDEX_READ,
					getSecondaryKeyOfPrimaryKeySql(secondaryIndexTableName), 
					new Object[] { primaryIndexKey }, 
					RowMappers.newObjectRowMapper(secondaryIndex.getColumnInfo().getColumnType()),
					getJdbcTemplate());
		
		try{
			return proxy.execute();
		} catch(EmptyResultDataAccessException e) {
			throw new HiveKeyNotFoundException(String.format("Unable to get secondary index keys of primary index key %s, key not found.", primaryIndexKey), primaryIndexKey,e);
		}
	}
	
	/**
	 * SQL string methods
	 */
	private String insertPrimaryIndexSql(PartitionDimension partitionDimension) {
		return "insert into " + IndexSchema.getPrimaryIndexTableName(partitionDimension)
		+ " (id, node, read_only, secondary_index_count, last_updated)"
		+ " values(?, ?, 0, 0, ?)";
	}
	private String deletePrimaryIndexSql(PartitionDimension partitionDimension) {
		return "delete from " + IndexSchema.getPrimaryIndexTableName(partitionDimension) + " where id = ?";
	}
	private String insertSecondaryIndexKeySql(SecondaryIndex secondaryIndex, PartitionDimension partitionDimension) {
		return "insert into " + IndexSchema.getSecondaryIndexTableName(partitionDimension, secondaryIndex)
			+ " (id, pkey)"
			+ " values(?, ?)";
	}
	private String updateSecondaryIndexSql(final SecondaryIndex secondaryIndex) {
		return "update " + IndexSchema.getSecondaryIndexTableName(secondaryIndex.getResource().getPartitionDimension(), secondaryIndex)
			+ " set pkey = ? where id = ? and pkey = ?";
	}
	private String deleteSecondaryIndexKeysSql(SecondaryIndex secondaryIndex) {
		return "delete from " + IndexSchema.getSecondaryIndexTableName(partitionDimension, secondaryIndex)
			+ " where pkey = ?";
	}
	private String deleteSingleSecondaryIndexKey(final SecondaryIndex secondaryIndex) {
		return "delete from " + IndexSchema.getSecondaryIndexTableName(partitionDimension, secondaryIndex)
			+ " where id = ? and pkey = ?";
	}
	private String selectNodeIdsByPrimaryIndexSql() {
		return "select node from " + IndexSchema.getPrimaryIndexTableName(partitionDimension)														 
				 + " where id = ?";
	}
	private String checkExistenceOfPrimaryKeySql() {
		return "select id from " + IndexSchema.getPrimaryIndexTableName(partitionDimension)														 
				+ " where id =  ?";
	}	
	private class NodeSemaphoreRowMapper implements ParameterizedRowMapper {
		public Object mapRow(ResultSet rs, int arg1) throws SQLException {
			return new NodeSemaphore(rs.getInt("node"), rs.getBoolean("read_only"));
		}	
	}
	private String selectNodeSemaphoreOfPrimaryIndexKeySql(PartitionDimension partitionDimension) {
		return "select node,read_only from " + IndexSchema.getPrimaryIndexTableName(partitionDimension)														 
				 + " where id = ?";
	}
	private String getReadOnlyOfPrimaryndexKeySql(PartitionDimension partitionDimension){
		return "select read_only from " + IndexSchema.getPrimaryIndexTableName(partitionDimension)														 
		 + " where id =  ?";
	}
	private String checkExistenceOfSecondaryIndexSql(final SecondaryIndex secondaryIndex) {
		return "select id from " + IndexSchema.getSecondaryIndexTableName(secondaryIndex.getResource().getPartitionDimension(), secondaryIndex)
				+ " where id = ?";
	}
	private String getNodeIdsOfSecondaryIndexKeySql(final SecondaryIndex secondaryIndex) {
		return "select p.node from " + IndexSchema.getPrimaryIndexTableName(secondaryIndex.getResource().getPartitionDimension()) + " p"	
				+ " join " + IndexSchema.getSecondaryIndexTableName(secondaryIndex.getResource().getPartitionDimension(), secondaryIndex) + " s on s.pkey = p.id"
				+ " where s.id =  ?";
	}
	private String getNodeSemaphoreOfSecondaryIndexKeySql(final SecondaryIndex secondaryIndex) {
		return "select p.node,p.read_only from " + IndexSchema.getPrimaryIndexTableName(partitionDimension) + " p"	
				+ " join " + IndexSchema.getSecondaryIndexTableName(partitionDimension, secondaryIndex) + " s on s.pkey = p.id"
				+ " where s.id =  ?";
	}
	private String getPrimaryIndexKeysOfSecondaryIndexKeySql(final SecondaryIndex secondaryIndex) {
		return "select p.id from " + IndexSchema.getPrimaryIndexTableName(partitionDimension) + " p"	
				+ " join " + IndexSchema.getSecondaryIndexTableName(partitionDimension, secondaryIndex) + " s on s.pkey = p.id"
				+ " where s.id =  ?";
	}
	
	private Collection<Integer> getIds(Collection<Node> nodes) {
		return Transform.map(new Unary<Node, Integer>(){
			public Integer f(Node item) {
				return item.getId();
			}}, nodes);
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

	private String getSecondaryKeyOfPrimaryKeySql(final String secondaryIndexTableName) {
		return "select s.id from " + IndexSchema.getPrimaryIndexTableName(partitionDimension) + " p"	
				+ " join " + secondaryIndexTableName + " s on s.pkey = p.id"
				+ " where p.id = ?";
	}
}
