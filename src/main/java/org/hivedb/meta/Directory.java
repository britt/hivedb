package org.hivedb.meta;

import static org.hivedb.management.statistics.DirectoryPerformanceStatisticsMBean.PRIMARYINDEXDELETECOUNT;
import static org.hivedb.management.statistics.DirectoryPerformanceStatisticsMBean.PRIMARYINDEXDELETEFAILURES;
import static org.hivedb.management.statistics.DirectoryPerformanceStatisticsMBean.PRIMARYINDEXDELETETIME;
import static org.hivedb.management.statistics.DirectoryPerformanceStatisticsMBean.PRIMARYINDEXREADCOUNT;
import static org.hivedb.management.statistics.DirectoryPerformanceStatisticsMBean.PRIMARYINDEXREADFAILURES;
import static org.hivedb.management.statistics.DirectoryPerformanceStatisticsMBean.PRIMARYINDEXREADTIME;
import static org.hivedb.management.statistics.DirectoryPerformanceStatisticsMBean.PRIMARYINDEXWRITECOUNT;
import static org.hivedb.management.statistics.DirectoryPerformanceStatisticsMBean.PRIMARYINDEXWRITEFAILURES;
import static org.hivedb.management.statistics.DirectoryPerformanceStatisticsMBean.PRIMARYINDEXWRITETIME;
import static org.hivedb.management.statistics.DirectoryPerformanceStatisticsMBean.SECONDARYINDEXDELETECOUNT;
import static org.hivedb.management.statistics.DirectoryPerformanceStatisticsMBean.SECONDARYINDEXDELETEFAILURES;
import static org.hivedb.management.statistics.DirectoryPerformanceStatisticsMBean.SECONDARYINDEXDELETETIME;
import static org.hivedb.management.statistics.DirectoryPerformanceStatisticsMBean.SECONDARYINDEXREADCOUNT;
import static org.hivedb.management.statistics.DirectoryPerformanceStatisticsMBean.SECONDARYINDEXREADFAILURES;
import static org.hivedb.management.statistics.DirectoryPerformanceStatisticsMBean.SECONDARYINDEXREADTIME;
import static org.hivedb.management.statistics.DirectoryPerformanceStatisticsMBean.SECONDARYINDEXWRITECOUNT;
import static org.hivedb.management.statistics.DirectoryPerformanceStatisticsMBean.SECONDARYINDEXWRITEFAILURES;
import static org.hivedb.management.statistics.DirectoryPerformanceStatisticsMBean.SECONDARYINDEXWRITETIME;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collection;

import javax.sql.DataSource;

import org.hivedb.HiveException;
import org.hivedb.StatisticsProxy;
import org.hivedb.management.statistics.Counter;
import org.hivedb.management.statistics.NoOpStatistics;
import org.hivedb.util.JdbcTypeMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreatorFactory;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.support.JdbcDaoSupport;

public class Directory extends JdbcDaoSupport {
	private PartitionDimension partitionDimension;
	private Counter performanceStatistics;
	private boolean performanceMonitoringEnabled = true;
	
	public Directory(PartitionDimension dimension, DataSource dataSource) {
		this.partitionDimension = dimension;
		this.setDataSource(dataSource);
		this.performanceStatistics = new NoOpStatistics();
	}
	
	public PartitionDimension getPartitionDimension() {
		return this.partitionDimension;
	}
	
	public void insertPrimaryIndexKey(final Node node, final Object primaryIndexKey) throws HiveException {
		final JdbcTemplate j = getJdbcTemplate();
		final Object[] parameters = new Object[] {
			primaryIndexKey,
			node.getId(),
			new Date(System.currentTimeMillis()) 
		};
		
		final PreparedStatementCreatorFactory creatorFactory = new PreparedStatementCreatorFactory("insert into " + IndexSchema.getPrimaryIndexTableName(partitionDimension)
			+ " (id, node, read_only, secondary_index_count, last_updated)"
			+ " values(?, ?, 0, 0, ?)",
			new int[] { 
				JdbcTypeMapper.primitiveTypeToJdbcType(primaryIndexKey.getClass()),
				Types.INTEGER,
				Types.DATE
		});
		
		StatisticsProxy<Object, HiveException> proxy = 
			new StatisticsProxy<Object, HiveException>(performanceStatistics, PRIMARYINDEXWRITECOUNT, PRIMARYINDEXWRITEFAILURES, PRIMARYINDEXWRITETIME) {
			
			@Override
			protected Object doWork() throws HiveException {
				if (j.update(creatorFactory.newPreparedStatementCreator(parameters)) != 1) 
					throw new HiveException(String.format("Unable to insert primary index key %s onto node %s", primaryIndexKey, node.getId()));
				return false;
			}
		};
		
		proxy.execute();
	}

	public void insertSecondaryIndexKey(final SecondaryIndex secondaryIndex, final Object secondaryIndexKey, Object primaryindexKey) throws HiveException {
		final JdbcTemplate j = getJdbcTemplate();
		final Object[] parameters = new Object[] {
			secondaryIndexKey,
			primaryindexKey
		};
		final PreparedStatementCreatorFactory creatorFactory = new PreparedStatementCreatorFactory("insert into " + IndexSchema.getSecondaryIndexTableName(partitionDimension, secondaryIndex)
			+ " (id, pkey)"
			+ " values(?, ?)",
			new int[] {
				JdbcTypeMapper.primitiveTypeToJdbcType(secondaryIndexKey.getClass()),
				JdbcTypeMapper.primitiveTypeToJdbcType(primaryindexKey.getClass()),
		});
		
		StatisticsProxy<Object, HiveException> proxy = 
			new StatisticsProxy<Object, HiveException>(performanceStatistics, SECONDARYINDEXWRITECOUNT, SECONDARYINDEXWRITEFAILURES, SECONDARYINDEXWRITETIME) {
			
				@Override
				protected Object doWork() throws HiveException {
					if (j.update(creatorFactory.newPreparedStatementCreator(parameters)) != 1) 
						throw new HiveException(
								String.format(
										"Unable to insert secondary index key %s onto secondaryIndex %s", 
										secondaryIndexKey, 
										secondaryIndex.getName()));
					return false;
				}
		};
		
		proxy.execute();
	}
	
	public void updatePrimaryIndexKey(final Node node, final Object primaryIndexKey) throws HiveException {
		final JdbcTemplate j = getJdbcTemplate();
		final Object[] parameters = new Object[] {
				node.getId(),
				primaryIndexKey 
		};
		final PreparedStatementCreatorFactory creatorFactory = new PreparedStatementCreatorFactory("update " + IndexSchema.getPrimaryIndexTableName(partitionDimension)
			+ " set node = ? where id = ?",
			new int[] {
					Types.INTEGER,
					JdbcTypeMapper.primitiveTypeToJdbcType(primaryIndexKey.getClass())
		});
		
		StatisticsProxy<Object, HiveException> proxy = new StatisticsProxy<Object, HiveException>(performanceStatistics, PRIMARYINDEXWRITECOUNT, PRIMARYINDEXWRITEFAILURES, PRIMARYINDEXWRITETIME) {

			@Override
			protected Object doWork() throws HiveException {
				if (j.update(creatorFactory.newPreparedStatementCreator(parameters)) != 1)
					throw new HiveException(String.format("Unable to update primary index key %s to node %s", primaryIndexKey, node.getId()));
				return null;
			}
			
		};
		proxy.execute();
	}
	
	public void updatePrimaryIndexKeyReadOnly(final Object primaryIndexKey, boolean isReadOnly) throws HiveException {
		final JdbcTemplate j = getJdbcTemplate();
		final Object[] parameters = new Object[] {
				isReadOnly,
				primaryIndexKey 
		};
		final PreparedStatementCreatorFactory creatorFactory = new PreparedStatementCreatorFactory("update " + IndexSchema.getPrimaryIndexTableName(partitionDimension)
			+ " set read_only = ? where id = ?",
			new int[] {
					Types.BOOLEAN,
					JdbcTypeMapper.primitiveTypeToJdbcType(primaryIndexKey.getClass())
		});
		
		
		StatisticsProxy<Object, HiveException> proxy = new StatisticsProxy<Object, HiveException>(performanceStatistics, PRIMARYINDEXWRITECOUNT, PRIMARYINDEXWRITEFAILURES, PRIMARYINDEXWRITETIME) {

			@Override
			protected Object doWork() throws HiveException {
				if (j.update(creatorFactory.newPreparedStatementCreator(parameters)) != 1)
					throw new HiveException(String.format("Unable to update read only status primary index key %s", primaryIndexKey));
				return null;
			}
			
		};
		
		proxy.execute();
	}
	
	public void updatePrimaryIndexOfSecondaryKey(final SecondaryIndex secondaryIndex, final Object secondaryIndexKey, final Object primaryIndexKey) throws HiveException {
		final JdbcTemplate j = getJdbcTemplate();
		final Object[] parameters = new Object[] {
			primaryIndexKey,
			secondaryIndexKey
		};
		final PreparedStatementCreatorFactory creatorFactory = new PreparedStatementCreatorFactory("update " + IndexSchema.getSecondaryIndexTableName(partitionDimension, secondaryIndex)
			+ " set pkey = ? where id = ?",
			new int[] {
				JdbcTypeMapper.primitiveTypeToJdbcType(primaryIndexKey.getClass()),
				JdbcTypeMapper.primitiveTypeToJdbcType(secondaryIndexKey.getClass())
		});
		
		StatisticsProxy<Object, HiveException> proxy = new StatisticsProxy<Object, HiveException>(performanceStatistics, SECONDARYINDEXWRITECOUNT, SECONDARYINDEXWRITEFAILURES, SECONDARYINDEXWRITETIME){
			@Override
			protected Object doWork() throws HiveException {
				if (j.update(creatorFactory.newPreparedStatementCreator(parameters)) != 1)
					throw new HiveException(String.format("Unable to update secondary index key %s to primary index key %s on secondary index", secondaryIndexKey, primaryIndexKey, secondaryIndex.getName()));
				return null;
			}
		};
		proxy.execute();
	}
	
	public void deleteAllSecondaryIndexKeysOfPrimaryIndexKey(SecondaryIndex secondaryIndex, Object primaryIndexKey) throws HiveException {
		final JdbcTemplate j = getJdbcTemplate();
		final Object[] parameters = new Object[] {
			primaryIndexKey 
		};
		final PreparedStatementCreatorFactory creatorFactory = new PreparedStatementCreatorFactory("delete from " + IndexSchema.getSecondaryIndexTableName(partitionDimension, secondaryIndex)
			+ " where pkey = ?",
			new int[] {
				JdbcTypeMapper.primitiveTypeToJdbcType(primaryIndexKey.getClass())
		});
		
		StatisticsProxy<Integer, HiveException> proxy = new StatisticsProxy<Integer, HiveException>(performanceStatistics, SECONDARYINDEXDELETECOUNT, SECONDARYINDEXDELETEFAILURES, SECONDARYINDEXDELETETIME) {

			@Override
			protected Integer doWork() throws HiveException {
				return j.update(creatorFactory.newPreparedStatementCreator(parameters));
			}
			
			@Override
			protected void onSuccess(Integer output) {
				counter.add(successKey,output);
				counter.add(timeKey, getRuntimeInMillis());
			}
			
		};
		proxy.execute();
	}
	
	public void deletePrimaryIndexKey(final Object primaryIndexKey) throws HiveException {
		final JdbcTemplate j = getJdbcTemplate();
		final Object[] parameters = new Object[] {
			primaryIndexKey 
		};
		final PreparedStatementCreatorFactory creatorFactory = new PreparedStatementCreatorFactory("delete from " + IndexSchema.getPrimaryIndexTableName(partitionDimension)
			+ " where id = ?",
			new int[] {
				JdbcTypeMapper.primitiveTypeToJdbcType(primaryIndexKey.getClass())
		});
		
		StatisticsProxy<Object, HiveException> proxy = new StatisticsProxy<Object, HiveException>(performanceStatistics, PRIMARYINDEXDELETECOUNT, PRIMARYINDEXDELETEFAILURES, PRIMARYINDEXDELETETIME){
			@Override
			protected Object doWork() throws HiveException {
				if (j.update(creatorFactory.newPreparedStatementCreator(parameters)) != 1)
					throw new HiveException(String.format("Unable to delete primary index key %s on partitoin dimension %s", primaryIndexKey, partitionDimension.getName()));
				return null;
			}
		};
		proxy.execute();
	}

	public void deleteSecondaryIndexKey(final SecondaryIndex secondaryIndex, final Object secondaryIndexKey) throws HiveException {
		final JdbcTemplate j = getJdbcTemplate();
		final Object[] parameters = new Object[] {
			secondaryIndexKey 
		};
		final PreparedStatementCreatorFactory creatorFactory = new PreparedStatementCreatorFactory("delete from " + IndexSchema.getSecondaryIndexTableName(partitionDimension, secondaryIndex)
			+ " where id = ?",
			new int[] {
				JdbcTypeMapper.primitiveTypeToJdbcType(secondaryIndexKey.getClass())
		});
		
		StatisticsProxy<Integer, HiveException> proxy = 
			new StatisticsProxy<Integer, HiveException>(performanceStatistics, SECONDARYINDEXDELETECOUNT, SECONDARYINDEXDELETEFAILURES, SECONDARYINDEXDELETETIME) {
			@Override
			protected Integer doWork() throws HiveException {
				if (j.update(creatorFactory.newPreparedStatementCreator(parameters)) != 1)
					throw new HiveException(String.format("Unable to delete secondary index key %s on secondary index %s", secondaryIndexKey, secondaryIndex.getName()));
				return null;
			} 
		};
		proxy.execute();
	}
	
	public boolean doesPrimaryIndexKeyExist(final Object primaryIndexKey) {
		final JdbcTemplate j = getJdbcTemplate();
		
		StatisticsProxy<Boolean, RuntimeException> proxy = new StatisticsProxy<Boolean, RuntimeException>(performanceStatistics, PRIMARYINDEXREADCOUNT, PRIMARYINDEXREADFAILURES, PRIMARYINDEXREADTIME) {

			@Override
			protected Boolean doWork() throws RuntimeException {
				return j.query("select id from " + IndexSchema.getPrimaryIndexTableName(partitionDimension)														 
						+ " where id =  ?",		
						new Object[] { primaryIndexKey },
						new IntRowMapper()).size() == 1;
			}
			
		};
		
		return proxy.execute();
	}
	
	public int getNodeIdOfPrimaryIndexKey(final Object primaryIndexKey) {
		final JdbcTemplate j = getJdbcTemplate();
		StatisticsProxy<Integer, RuntimeException> proxy = new StatisticsProxy<Integer, RuntimeException>(performanceStatistics, PRIMARYINDEXREADCOUNT, PRIMARYINDEXREADFAILURES, PRIMARYINDEXREADTIME) {
			@Override
			protected Integer doWork() throws RuntimeException {
				return j.queryForInt("select node from " + IndexSchema.getPrimaryIndexTableName(partitionDimension)														 
						 + " where id = ?",		
							new Object[] { primaryIndexKey });
			}
		};
		return proxy.execute();
	}
	
	public NodeSemaphore getNodeSemamphoreOfPrimaryIndexKey(final Object primaryIndexKey) {
		final JdbcTemplate j = getJdbcTemplate();
		StatisticsProxy<NodeSemaphore, RuntimeException> proxy = 
			new StatisticsProxy<NodeSemaphore, RuntimeException>(performanceStatistics, PRIMARYINDEXREADCOUNT, PRIMARYINDEXREADFAILURES, PRIMARYINDEXREADTIME){
			@Override
			protected NodeSemaphore doWork() throws RuntimeException {
				return (NodeSemaphore) j.queryForObject("select node,read_only from " + IndexSchema.getPrimaryIndexTableName(partitionDimension)														 
						 + " where id = ?",		
							new Object[] { primaryIndexKey },
							new NodeSemaphoreRowMapper());
			}
		};
		return proxy.execute();
	}

	@SuppressWarnings("unchecked")
	public boolean getReadOnlyOfPrimaryIndexKey(final Object primaryIndexKey) {
		final JdbcTemplate j = getJdbcTemplate();
		StatisticsProxy<Boolean, RuntimeException> proxy = new StatisticsProxy<Boolean, RuntimeException>(performanceStatistics, PRIMARYINDEXREADCOUNT, PRIMARYINDEXREADFAILURES, PRIMARYINDEXREADTIME) {
			@Override
			protected Boolean doWork() throws RuntimeException {
				return (Boolean)j.queryForObject("select read_only from " + IndexSchema.getPrimaryIndexTableName(partitionDimension)														 
						 + " where id =  ?",		
							new Object[] { primaryIndexKey },
							new BooleanRowMapper());
			}
		};
		return proxy.execute();
	}

	public boolean doesSecondaryIndexKeyExist(final SecondaryIndex secondaryIndex,final Object secondaryIndexKey) {
		final JdbcTemplate j = getJdbcTemplate();
		StatisticsProxy<Boolean, RuntimeException> proxy = new StatisticsProxy<Boolean, RuntimeException>(performanceStatistics, SECONDARYINDEXREADCOUNT, SECONDARYINDEXREADFAILURES, SECONDARYINDEXREADTIME) {
			@Override
			protected Boolean doWork() throws RuntimeException {
				return j.query("select id from " + IndexSchema.getSecondaryIndexTableName(partitionDimension, secondaryIndex)
						+ " where id = ?",
						new Object[] { secondaryIndexKey },
						new TrueRowMapper()).size() == 1;
			}
		};
		return proxy.execute();
	}

	@SuppressWarnings("unchecked")
	public Integer getNodeIdOfSecondaryIndexKey(final SecondaryIndex secondaryIndex, final Object secondaryIndexKey)
	{
		final JdbcTemplate j = getJdbcTemplate();
		StatisticsProxy<Integer, RuntimeException> proxy = new StatisticsProxy<Integer, RuntimeException>(performanceStatistics, SECONDARYINDEXREADCOUNT, SECONDARYINDEXREADFAILURES, SECONDARYINDEXREADTIME) {
			@Override
			protected Integer doWork() throws RuntimeException {
				return j.queryForInt("select p.node from " + IndexSchema.getPrimaryIndexTableName(partitionDimension) + " p"	
						+ " join " + IndexSchema.getSecondaryIndexTableName(partitionDimension, secondaryIndex) + " s on s.pkey = p.id"
						+ " where s.id =  ?",
						new Object[] { secondaryIndexKey });
			}
		};
		return proxy.execute();
	}
	
	public NodeSemaphore getNodeSemaphoreOfSecondaryIndexKey(final SecondaryIndex secondaryIndex, final Object secondaryIndexKey)
	{
		final JdbcTemplate j = getJdbcTemplate();
		StatisticsProxy<NodeSemaphore, RuntimeException> proxy = new StatisticsProxy<NodeSemaphore, RuntimeException>(performanceStatistics, SECONDARYINDEXREADCOUNT, SECONDARYINDEXREADFAILURES, SECONDARYINDEXREADTIME) {
			@Override
			protected NodeSemaphore doWork() throws RuntimeException {
				return (NodeSemaphore) j.queryForObject("select p.node,p.read_only from " + IndexSchema.getPrimaryIndexTableName(partitionDimension) + " p"	
						+ " join " + IndexSchema.getSecondaryIndexTableName(partitionDimension, secondaryIndex) + " s on s.pkey = p.id"
						+ " where s.id =  ?",
						new Object[] { secondaryIndexKey },
						new NodeSemaphoreRowMapper());
			}
		};
		return proxy.execute();
	}
	
	@SuppressWarnings("unchecked")
	public Object getPrimaryIndexKeyOfSecondaryIndexKey(final SecondaryIndex secondaryIndex, final Object secondaryIndexKey)
	{
		final JdbcTemplate j = getJdbcTemplate();
		StatisticsProxy<Object, RuntimeException> proxy = new StatisticsProxy<Object, RuntimeException>(performanceStatistics, SECONDARYINDEXREADCOUNT, SECONDARYINDEXREADFAILURES, SECONDARYINDEXREADTIME) {
			@Override
			protected Object doWork() throws RuntimeException {
				try {
				return j.queryForObject("select p.id from " + IndexSchema.getPrimaryIndexTableName(partitionDimension) + " p"	
						+ " join " + IndexSchema.getSecondaryIndexTableName(partitionDimension, secondaryIndex) + " s on s.pkey = p.id"
						+ " where s.id =  ?",
						new Object[] { secondaryIndexKey },
						new ObjectRowMapper(secondaryIndex.getResource().getPartitionDimension().getColumnType()));
				}
				catch (RuntimeException e) {
					throw new RuntimeException(String.format("Error looking for key %s of secondary index %s", secondaryIndexKey, secondaryIndex.getName()), e);
				}
			}
		};
		return proxy.execute();
	}
	
	@SuppressWarnings("unchecked")
	public Collection getSecondaryIndexKeysOfPrimaryIndexKey(final SecondaryIndex secondaryIndex, final Object primaryIndexKey)
	{
		final JdbcTemplate j = getJdbcTemplate();
		final String secondaryIndexTableName = IndexSchema.getSecondaryIndexTableName(partitionDimension, secondaryIndex);
		StatisticsProxy<Collection, RuntimeException> proxy = new StatisticsProxy<Collection, RuntimeException>(performanceStatistics, SECONDARYINDEXREADCOUNT, SECONDARYINDEXREADFAILURES, SECONDARYINDEXREADTIME) {
			@Override
			protected Collection doWork() throws RuntimeException {
				return j.query("select s.id from " + IndexSchema.getPrimaryIndexTableName(partitionDimension) + " p"	
						+ " join " + secondaryIndexTableName + " s on s.pkey = p.id"
						+ " where p.id = ?",
						new Object[] { primaryIndexKey },
						new ObjectRowMapper(secondaryIndex.getColumnInfo().getColumnType()));
			}
			
			@Override
			protected void onSuccess(Collection output) {
				counter.add(successKey, output.size());
				counter.add(timeKey, getRuntimeInMillis());
			}
		};	
		return proxy.execute();
	}
	
	private class IntRowMapper implements RowMapper {
		public Object mapRow(ResultSet rs, int rowNumber) throws SQLException {
			return rs.getInt(1);		
		}
	}
	private class BooleanRowMapper implements RowMapper {
		public Object mapRow(ResultSet rs, int rowNumber) throws SQLException {
			return rs.getBoolean(1);		
		}
	}
	private class ObjectRowMapper implements RowMapper {
		int jdbcType;
		public ObjectRowMapper(int jdbcType)
		{
			this.jdbcType = jdbcType;
		}
		public Object mapRow(ResultSet rs, int rowNumber) throws SQLException {
			JdbcTypeMapper.getJdbcTypeResult(rs, 1, jdbcType);
			return rs.getObject(1);		
		}
	}
	private static class TrueRowMapper implements RowMapper {
		public Object mapRow(ResultSet rs, int rowNumber) throws SQLException {
			return true;
		}
	}
	
	private class NodeSemaphoreRowMapper implements RowMapper {
		public Object mapRow(ResultSet rs, int arg1) throws SQLException {
			return new NodeSemaphore(rs.getInt("node"), rs.getBoolean("read_only"));
		}	
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
}
