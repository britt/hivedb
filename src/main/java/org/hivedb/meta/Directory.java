package org.hivedb.meta;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collection;

import javax.sql.DataSource;

import org.hivedb.HiveException;
import org.hivedb.util.JdbcTypeMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreatorFactory;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.support.JdbcDaoSupport;

public class Directory extends JdbcDaoSupport{
	private PartitionDimension partitionDimension;
	
	public Directory(PartitionDimension dimension, DataSource dataSource) {
		this.partitionDimension = dimension;
		this.setDataSource(dataSource);
	}
	
	public PartitionDimension getPartitionDimension() {
		return this.partitionDimension;
	}
	
	public void insertPrimaryIndexKey(Node node, Object primaryIndexKey) throws HiveException {
		JdbcTemplate j = getJdbcTemplate();
		Object[] parameters = new Object[] {
			primaryIndexKey,
			node.getId(),
			new Date(System.currentTimeMillis()) 
		};
		PreparedStatementCreatorFactory creatorFactory = new PreparedStatementCreatorFactory("insert into " + IndexSchema.getPrimaryIndexTableName(partitionDimension)
			+ " (id, node, read_only, secondary_index_count, last_updated)"
			+ " values(?, ?, 0, 0, ?)",
			new int[] { 
				JdbcTypeMapper.primitiveTypeToJdbcType(primaryIndexKey.getClass()),
				Types.INTEGER,
				Types.DATE
		});
		if (j.update(creatorFactory.newPreparedStatementCreator(parameters)) != 1)
			throw new HiveException(String.format("Unable to insert primary index key %s onto node %s", primaryIndexKey, node.getId()));
	}

	public void insertSecondaryIndexKey(SecondaryIndex secondaryIndex, Object secondaryIndexKey, Object primaryindexKey) throws HiveException {
		JdbcTemplate j = getJdbcTemplate();
		Object[] parameters = new Object[] {
			secondaryIndexKey,
			primaryindexKey
		};
		PreparedStatementCreatorFactory creatorFactory = new PreparedStatementCreatorFactory("insert into " + IndexSchema.getSecondaryIndexTableName(partitionDimension, secondaryIndex)
			+ " (id, pkey)"
			+ " values(?, ?)",
			new int[] {
				JdbcTypeMapper.primitiveTypeToJdbcType(secondaryIndexKey.getClass()),
				JdbcTypeMapper.primitiveTypeToJdbcType(primaryindexKey.getClass()),
		});
		if (j.update(creatorFactory.newPreparedStatementCreator(parameters)) != 1)
			throw new HiveException(String.format("Unable to insert secondary index key %s onto secondaryIndex %s", secondaryIndexKey, secondaryIndex.getName()));
	}
	
	public void updatePrimaryIndexKey(Node node, Object primaryIndexKey) throws HiveException {
		JdbcTemplate j = getJdbcTemplate();
		Object[] parameters = new Object[] {
				node.getId(),
				primaryIndexKey 
		};
		PreparedStatementCreatorFactory creatorFactory = new PreparedStatementCreatorFactory("update " + IndexSchema.getPrimaryIndexTableName(partitionDimension)
			+ " set node = ? where id = ?",
			new int[] {
					Types.INTEGER,
					JdbcTypeMapper.primitiveTypeToJdbcType(primaryIndexKey.getClass())
		});
		if (j.update(creatorFactory.newPreparedStatementCreator(parameters)) != 1)
			throw new HiveException(String.format("Unable to update primary index key %s to node %s", primaryIndexKey, node.getId()));
	}
	
	public void updatePrimaryIndexKeyReadOnly(Object primaryIndexKey, boolean isReadOnly) throws HiveException {
		JdbcTemplate j = getJdbcTemplate();
		Object[] parameters = new Object[] {
				isReadOnly,
				primaryIndexKey 
		};
		PreparedStatementCreatorFactory creatorFactory = new PreparedStatementCreatorFactory("update " + IndexSchema.getPrimaryIndexTableName(partitionDimension)
			+ " set read_only = ? where id = ?",
			new int[] {
					Types.BOOLEAN,
					JdbcTypeMapper.primitiveTypeToJdbcType(primaryIndexKey.getClass())
		});
		if (j.update(creatorFactory.newPreparedStatementCreator(parameters)) != 1)
			throw new HiveException(String.format("Unable to update read only status primary index key %s", primaryIndexKey));
	}
	
	public void updatePrimaryIndexOfSecondaryKey(SecondaryIndex secondaryIndex, Object secondaryIndexKey, Object primaryIndexKey) throws HiveException {
		JdbcTemplate j = getJdbcTemplate();
		Object[] parameters = new Object[] {
			primaryIndexKey,
			secondaryIndexKey
		};
		PreparedStatementCreatorFactory creatorFactory = new PreparedStatementCreatorFactory("update " + IndexSchema.getSecondaryIndexTableName(partitionDimension, secondaryIndex)
			+ " set pkey = ? where id = ?",
			new int[] {
				JdbcTypeMapper.primitiveTypeToJdbcType(primaryIndexKey.getClass()),
				JdbcTypeMapper.primitiveTypeToJdbcType(secondaryIndexKey.getClass())
		});
		if (j.update(creatorFactory.newPreparedStatementCreator(parameters)) != 1)
			throw new HiveException(String.format("Unable to update secondary index key %s to primary index key %s on secondary index", secondaryIndexKey, primaryIndexKey, secondaryIndex.getName()));
	}
	
	public void deleteAllSecondaryIndexKeysOfPrimaryIndexKey(SecondaryIndex secondaryIndex, Object primaryIndexKey) throws HiveException {
		JdbcTemplate j = getJdbcTemplate();
		Object[] parameters = new Object[] {
			primaryIndexKey 
		};
		PreparedStatementCreatorFactory creatorFactory = new PreparedStatementCreatorFactory("delete from " + IndexSchema.getSecondaryIndexTableName(partitionDimension, secondaryIndex)
			+ " where pkey = ?",
			new int[] {
				JdbcTypeMapper.primitiveTypeToJdbcType(primaryIndexKey.getClass())
		});
		j.update(creatorFactory.newPreparedStatementCreator(parameters));
	}
	
	public void deletePrimaryIndexKey(Object primaryIndexKey) throws HiveException {
		JdbcTemplate j = getJdbcTemplate();
		Object[] parameters = new Object[] {
			primaryIndexKey 
		};
		PreparedStatementCreatorFactory creatorFactory = new PreparedStatementCreatorFactory("delete from " + IndexSchema.getPrimaryIndexTableName(partitionDimension)
			+ " where id = ?",
			new int[] {
				JdbcTypeMapper.primitiveTypeToJdbcType(primaryIndexKey.getClass())
		});
		if (j.update(creatorFactory.newPreparedStatementCreator(parameters)) != 1)
			throw new HiveException(String.format("Unable to delete primary index key %s on partitoin dimension %s", primaryIndexKey, partitionDimension.getName()));
	}

	public void deleteSecondaryIndexKey(SecondaryIndex secondaryIndex, Object secondaryIndexKey) throws HiveException {
		JdbcTemplate j = getJdbcTemplate();
		Object[] parameters = new Object[] {
			secondaryIndexKey 
		};
		PreparedStatementCreatorFactory creatorFactory = new PreparedStatementCreatorFactory("delete from " + IndexSchema.getSecondaryIndexTableName(partitionDimension, secondaryIndex)
			+ " where id = ?",
			new int[] {
				JdbcTypeMapper.primitiveTypeToJdbcType(secondaryIndexKey.getClass())
		});
		if (j.update(creatorFactory.newPreparedStatementCreator(parameters)) != 1)
			throw new HiveException(String.format("Unable to delete secondary index key %s on secondary index %s", secondaryIndexKey, secondaryIndex.getName()));
	}
	
	public boolean doesPrimaryIndexKeyExist(Object primaryIndexKey) throws SQLException {
		JdbcTemplate j = getJdbcTemplate();
		return j.query("select id from " + IndexSchema.getPrimaryIndexTableName(partitionDimension)														 
			+ " where id =  ?",		
			new Object[] { primaryIndexKey },
			new IntRowMapper()).size() == 1;
	}
	
	public int getNodeIdOfPrimaryIndexKey(Object primaryIndexKey) {
		JdbcTemplate j = getJdbcTemplate();
		return j.queryForInt("select node from " + IndexSchema.getPrimaryIndexTableName(partitionDimension)														 
			 + " where id = ?",		
			new Object[] { primaryIndexKey });
	}
	
	public NodeSemaphore getNodeSemamphoreOfPrimaryIndexKey(Object primaryIndexKey) {
		JdbcTemplate j = getJdbcTemplate();
		return (NodeSemaphore) j.queryForObject("select node,read_only from " + IndexSchema.getPrimaryIndexTableName(partitionDimension)														 
				 + " where id = ?",		
				new Object[] { primaryIndexKey },
				new NodeSemaphoreRowMapper());
	}

	@SuppressWarnings("unchecked")
	public boolean getReadOnlyOfPrimaryIndexKey(Object primaryIndexKey) {
		JdbcTemplate j = getJdbcTemplate();
		return (Boolean)j.queryForObject("select read_only from " + IndexSchema.getPrimaryIndexTableName(partitionDimension)														 
			 + " where id =  ?",		
			new Object[] { primaryIndexKey },
			new BooleanRowMapper());
	}

	public boolean doesSecondaryIndexKeyExist(SecondaryIndex secondaryIndex, Object secondaryIndexKey) throws SQLException {
		JdbcTemplate j = getJdbcTemplate();
		return j.query("select id from " + IndexSchema.getSecondaryIndexTableName(partitionDimension, secondaryIndex)
			+ " where id = ?",
			new Object[] { secondaryIndexKey },
			new TrueRowMapper()).size() == 1;
	}

	@SuppressWarnings("unchecked")
	public Integer getNodeIdOfSecondaryIndexKey(SecondaryIndex secondaryIndex, Object secondaryIndexKey)
	{
		JdbcTemplate j = getJdbcTemplate();
		return j.queryForInt("select p.node from " + IndexSchema.getPrimaryIndexTableName(partitionDimension) + " p"	
			+ " join " + IndexSchema.getSecondaryIndexTableName(partitionDimension, secondaryIndex) + " s on s.pkey = p.id"
			+ " where s.id =  ?",
			new Object[] { secondaryIndexKey });
	}
	
	public NodeSemaphore getNodeSemaphoreOfSecondaryIndexKey(SecondaryIndex secondaryIndex, Object secondaryIndexKey)
	{
		JdbcTemplate j = getJdbcTemplate();
		return (NodeSemaphore) j.queryForObject("select p.node,p.read_only from " + IndexSchema.getPrimaryIndexTableName(partitionDimension) + " p"	
			+ " join " + IndexSchema.getSecondaryIndexTableName(partitionDimension, secondaryIndex) + " s on s.pkey = p.id"
			+ " where s.id =  ?",
			new Object[] { secondaryIndexKey },
			new NodeSemaphoreRowMapper());
	}
	
	@SuppressWarnings("unchecked")
	public Object getPrimaryIndexKeyOfSecondaryIndexKey(SecondaryIndex secondaryIndex, Object secondaryIndexKey)
	{
		JdbcTemplate j = getJdbcTemplate();
		return j.queryForObject("select p.id from " + IndexSchema.getPrimaryIndexTableName(partitionDimension) + " p"	
			+ " join " + IndexSchema.getSecondaryIndexTableName(partitionDimension, secondaryIndex) + " s on s.pkey = p.id"
			+ " where s.id =  ?",
			new Object[] { secondaryIndexKey },
			new ObjectRowMapper(secondaryIndex.getResource().getPartitionDimension().getColumnType()));
	}
	
	@SuppressWarnings("unchecked")
	public Collection getSecondaryIndexKeysOfPrimaryIndexKey(SecondaryIndex secondaryIndex, Object primaryIndexKey)
	{
		JdbcTemplate j = getJdbcTemplate();
		String secondaryIndexTableName = IndexSchema.getSecondaryIndexTableName(partitionDimension, secondaryIndex);
		return j.query("select s.id from " + IndexSchema.getPrimaryIndexTableName(partitionDimension) + " p"	
			+ " join " + secondaryIndexTableName + " s on s.pkey = p.id"
			+ " where p.id = ?",
			new Object[] { primaryIndexKey },
			new ObjectRowMapper(secondaryIndex.getColumnInfo().getColumnType()));
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
}
