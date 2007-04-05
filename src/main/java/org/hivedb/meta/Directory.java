package org.hivedb.meta;

import static org.hivedb.management.statistics.DirectoryPerformanceStatisticsMBean.*;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.management.NotCompliantMBeanException;
import javax.sql.DataSource;

import org.hivedb.HiveException;
import org.hivedb.StatisticsProxy;
import org.hivedb.Synchronizeable;
import org.hivedb.management.statistics.DirectoryPerformanceStatisticsMBean;
import org.hivedb.management.statistics.NodePerformanceStatisticsMBean;
import org.hivedb.util.JdbcTypeMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreatorFactory;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.support.JdbcDaoSupport;

public class Directory extends JdbcDaoSupport implements Synchronizeable {
	private PartitionDimension partitionDimension;
	private DirectoryPerformanceStatisticsMBean stats;
	private Map<Integer, NodePerformanceStatisticsMBean> nodeStats;
	private long interval = 100;
	private long window = 1000;
	
	public Directory(PartitionDimension dimension, DataSource dataSource) {
		this.partitionDimension = dimension;
		this.setDataSource(dataSource);
		this.nodeStats = new ConcurrentHashMap<Integer, NodePerformanceStatisticsMBean>();
//		 TODO Solve where to get these
		try {
			this.stats = new DirectoryPerformanceStatisticsMBean(1000,100);
			for(Node node : partitionDimension.getNodeGroup().getNodes())
				nodeStats.put(node.getId(), new NodePerformanceStatisticsMBean(window,interval));
		} catch (NotCompliantMBeanException e) {
			
		}
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
			new StatisticsProxy<Object, HiveException>(stats, PRIMARYINDEXWRITECOUNT, PRIMARYINDEXWRITEFAILURES, PRIMARYINDEXWRITETIME) {
			
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
			new StatisticsProxy<Object, HiveException>(stats, SECONDARYINDEXWRITECOUNT, SECONDARYINDEXWRITEFAILURES, SECONDARYINDEXWRITETIME) {
			
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
		
		StatisticsProxy<Object, HiveException> proxy = new StatisticsProxy<Object, HiveException>(stats, PRIMARYINDEXWRITECOUNT, PRIMARYINDEXWRITEFAILURES, PRIMARYINDEXWRITETIME) {

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
		
		
		StatisticsProxy<Object, HiveException> proxy = new StatisticsProxy<Object, HiveException>(stats, PRIMARYINDEXWRITECOUNT, PRIMARYINDEXWRITEFAILURES, PRIMARYINDEXWRITETIME) {

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
		
		StatisticsProxy<Object, HiveException> proxy = new StatisticsProxy<Object, HiveException>(stats, SECONDARYINDEXWRITECOUNT, SECONDARYINDEXWRITEFAILURES, SECONDARYINDEXWRITETIME){
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
		
		StatisticsProxy<Integer, HiveException> proxy = new StatisticsProxy<Integer, HiveException>(stats, SECONDARYINDEXDELETECOUNT, SECONDARYINDEXDELETEFAILURES, SECONDARYINDEXDELETETIME) {

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
		
		StatisticsProxy<Object, HiveException> proxy = new StatisticsProxy<Object, HiveException>(stats, PRIMARYINDEXDELETECOUNT, PRIMARYINDEXDELETEFAILURES, PRIMARYINDEXDELETETIME){
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
			new StatisticsProxy<Integer, HiveException>(stats, SECONDARYINDEXDELETECOUNT, SECONDARYINDEXDELETEFAILURES, SECONDARYINDEXDELETETIME) {
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
		
		StatisticsProxy<Boolean, RuntimeException> proxy = new StatisticsProxy<Boolean, RuntimeException>(stats, PRIMARYINDEXREADCOUNT, PRIMARYINDEXREADFAILURES, PRIMARYINDEXREADTIME) {

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
		StatisticsProxy<Integer, RuntimeException> proxy = new StatisticsProxy<Integer, RuntimeException>(stats, PRIMARYINDEXREADCOUNT, PRIMARYINDEXREADFAILURES, PRIMARYINDEXREADTIME) {
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
			new StatisticsProxy<NodeSemaphore, RuntimeException>(stats, PRIMARYINDEXREADCOUNT, PRIMARYINDEXREADFAILURES, PRIMARYINDEXREADTIME){
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
		StatisticsProxy<Boolean, RuntimeException> proxy = new StatisticsProxy<Boolean, RuntimeException>(stats, PRIMARYINDEXREADCOUNT, PRIMARYINDEXREADFAILURES, PRIMARYINDEXREADTIME) {
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
		StatisticsProxy<Boolean, RuntimeException> proxy = new StatisticsProxy<Boolean, RuntimeException>(stats, SECONDARYINDEXREADCOUNT, SECONDARYINDEXREADFAILURES, SECONDARYINDEXREADTIME) {
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
		StatisticsProxy<Integer, RuntimeException> proxy = new StatisticsProxy<Integer, RuntimeException>(stats, SECONDARYINDEXREADCOUNT, SECONDARYINDEXREADFAILURES, SECONDARYINDEXREADTIME) {
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
		StatisticsProxy<NodeSemaphore, RuntimeException> proxy = new StatisticsProxy<NodeSemaphore, RuntimeException>(stats, SECONDARYINDEXREADCOUNT, SECONDARYINDEXREADFAILURES, SECONDARYINDEXREADTIME) {
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
		StatisticsProxy<Object, RuntimeException> proxy = new StatisticsProxy<Object, RuntimeException>(stats, SECONDARYINDEXREADCOUNT, SECONDARYINDEXREADFAILURES, SECONDARYINDEXREADTIME) {
			@Override
			protected Object doWork() throws RuntimeException {
				return j.queryForObject("select p.id from " + IndexSchema.getPrimaryIndexTableName(partitionDimension) + " p"	
						+ " join " + IndexSchema.getSecondaryIndexTableName(partitionDimension, secondaryIndex) + " s on s.pkey = p.id"
						+ " where s.id =  ?",
						new Object[] { secondaryIndexKey },
						new ObjectRowMapper(secondaryIndex.getResource().getPartitionDimension().getColumnType()));
			}
		};
		return proxy.execute();
	}
	
	@SuppressWarnings("unchecked")
	public Collection getSecondaryIndexKeysOfPrimaryIndexKey(final SecondaryIndex secondaryIndex, final Object primaryIndexKey)
	{
		final JdbcTemplate j = getJdbcTemplate();
		final String secondaryIndexTableName = IndexSchema.getSecondaryIndexTableName(partitionDimension, secondaryIndex);
		StatisticsProxy<Collection, RuntimeException> proxy = new StatisticsProxy<Collection, RuntimeException>(stats, SECONDARYINDEXREADCOUNT, SECONDARYINDEXREADFAILURES, SECONDARYINDEXREADTIME) {
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

	public void sync() throws HiveException {
		// Merge Maps
		Collection<Integer> nodeIds = new ArrayList<Integer>();
		for(Node node : partitionDimension.getNodeGroup().getNodes()) {
			nodeIds.add(node.getId());
			if(!nodeStats.containsKey(node.getId()))
				try {
					nodeStats.put(node.getId(), new NodePerformanceStatisticsMBean(window, interval));
				} catch (NotCompliantMBeanException e) {

				}
		}
		//Exclude removed nodes
		for(Integer key: nodeStats.keySet()) {
			if(!nodeIds.contains(key))
				nodeStats.remove(key);
		}
	}
}
