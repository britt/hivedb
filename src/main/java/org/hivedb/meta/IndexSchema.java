/**
 * HiveDB is an Open Source (LGPL) system for creating large, high-transaction-volume
 * data storage systems.
 */
package org.hivedb.meta;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;

import org.hivedb.HiveException;
import org.hivedb.Schema;
import org.hivedb.util.JdbcTypeMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreatorFactory;
import org.springframework.jdbc.core.RowMapper;

/**
 * IndexSchema contains tables of primary and secondary indexes in
 * accordance with the rows existing in the Global Hive meta tables.
 * Each IndexSchema instance references a particular jdbc URI where it will
 * create index tables. All primary and secondary indexes of a partition
 * index must be stored at the same URI, hence you should always construct
 * and IndexSchema with the URI of a partition dimension's index node.
 * <p>
 * 
 * @author Andy Likuski (alikuski@cafepress.com)
 * @author Britt Crawford (bcrawford@cafepress.com)
 */
public class IndexSchema extends Schema{
	private PartitionDimension partitionDimension;
	/**
	 * IndexSchema is constructed against a JDBC URI, which will be the destination
	 * for the schema tables.
	 * 
	 * @param dbURI Empty target database connect string, including username, password & catalog
	 * @param dialect Data definition language dialect
	 */
	public IndexSchema(PartitionDimension partitionDimension) {
		super(partitionDimension.getIndexUri());
		this.partitionDimension = partitionDimension;
	}
	
	/**
	 * 
	 * @return
	 * @throws HiveException
	 */
	protected String getCreatePrimaryIndex() {
		return 
			"CREATE TABLE " + getPrimaryIndexTableName()
			+ " ( " 
			+ " id " + addLengthForVarchar(JdbcTypeMapper.jdbcTypeToString(partitionDimension.getColumnType())) + " primary key not null, "
			+ " node SMALLINT not null, "
			+ " secondary_index_count INTEGER not null, "
			+ " last_updated "+ JdbcTypeMapper.jdbcTypeToString(Types.DATE) +" not null, "
			+ " read_only " +  GlobalSchema.getBooleanTypeForDialect(dialect) + " default 0"			
			+ " )";
	}
	
	/**
	 * 
	 * @param secondaryIndex
	 * @throws HiveException
	 */
	protected String getCreateSecondaryIndex(SecondaryIndex secondaryIndex) {
		return 
			"CREATE TABLE " + getSecondaryIndexTableName(secondaryIndex) 
			+ " ( "
			+ " id " +  addLengthForVarchar(JdbcTypeMapper.jdbcTypeToString(secondaryIndex.getColumnInfo().getColumnType())) + " primary key not null, "
			+ " pkey " + addLengthForVarchar(JdbcTypeMapper.jdbcTypeToString(partitionDimension.getColumnType())) + " not null"
			+ " )";
	}
	
	/**
	 * Constructs the name of the table for the primary index.
	 * @return
	 */
	public String getPrimaryIndexTableName() {
		return "hive_primary_" + partitionDimension.getName().toLowerCase();
	}
	/**
	 * Constructs the name of the table for the secondary index.
	 * @return
	 */
	protected String getSecondaryIndexTableName(SecondaryIndex secondaryIndex) {
		return "hive_secondary_" + secondaryIndex.getResource().getName().toLowerCase() + "_" + secondaryIndex.getColumnInfo().getName();
	}
	
	@Override
	public Collection<TableInfo> getTables() {
		Collection<TableInfo> TableInfos = new ArrayList<TableInfo>();
		TableInfos.add(new TableInfo(getPrimaryIndexTableName(), getCreatePrimaryIndex()));
		for (Resource resource : partitionDimension.getResources())
			for (SecondaryIndex secondaryIndex : resource.getSecondaryIndexes())
				TableInfos.add(new TableInfo(
						getSecondaryIndexTableName(secondaryIndex), 
						getCreateSecondaryIndex(secondaryIndex)));
		return TableInfos;
	}
	
	public void insertPrimaryIndexKey(Node node, Object primaryIndexKey) throws HiveException {
		JdbcTemplate j = getJdbcTemplate();
		Object[] parameters = new Object[] {
			primaryIndexKey,
			node.getId(),
			new Date(System.currentTimeMillis()) 
		};
		PreparedStatementCreatorFactory creatorFactory = new PreparedStatementCreatorFactory("insert into " + getPrimaryIndexTableName()
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
		PreparedStatementCreatorFactory creatorFactory = new PreparedStatementCreatorFactory("insert into " + getSecondaryIndexTableName(secondaryIndex)
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
		PreparedStatementCreatorFactory creatorFactory = new PreparedStatementCreatorFactory("update " + getPrimaryIndexTableName()
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
		PreparedStatementCreatorFactory creatorFactory = new PreparedStatementCreatorFactory("update " + getPrimaryIndexTableName()
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
		PreparedStatementCreatorFactory creatorFactory = new PreparedStatementCreatorFactory("update " + getSecondaryIndexTableName(secondaryIndex)
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
		PreparedStatementCreatorFactory creatorFactory = new PreparedStatementCreatorFactory("delete from " + getSecondaryIndexTableName(secondaryIndex)
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
		PreparedStatementCreatorFactory creatorFactory = new PreparedStatementCreatorFactory("delete from " + getPrimaryIndexTableName()
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
		PreparedStatementCreatorFactory creatorFactory = new PreparedStatementCreatorFactory("delete from " + getSecondaryIndexTableName(secondaryIndex)
			+ " where id = ?",
			new int[] {
				JdbcTypeMapper.primitiveTypeToJdbcType(secondaryIndexKey.getClass())
		});
		if (j.update(creatorFactory.newPreparedStatementCreator(parameters)) != 1)
			throw new HiveException(String.format("Unable to delete secondary index key %s on secondary index %s", secondaryIndexKey, secondaryIndex.getName()));
	}
	
	public boolean doesPrimaryIndexKeyExist(Object primaryIndexKey) throws SQLException {
		JdbcTemplate j = getJdbcTemplate();
		return j.query("select id from " + getPrimaryIndexTableName()														 
			+ " where id =  ?",		
			new Object[] { primaryIndexKey },
			new IntRowMapper()).size() == 1;
	}

	
	public int getNodeIdOfPrimaryIndexKey(Object primaryIndexKey) {
		JdbcTemplate j = getJdbcTemplate();
		return j.queryForInt("select node from " + getPrimaryIndexTableName()														 
			 + " where id = ?",		
			new Object[] { primaryIndexKey });
	}

	@SuppressWarnings("unchecked")
	public boolean getReadOnlyOfPrimaryIndexKey(Object primaryIndexKey) {
		JdbcTemplate j = getJdbcTemplate();
		return (Boolean)j.queryForObject("select read_only from " + getPrimaryIndexTableName()														 
			 + " where id =  ?",		
			new Object[] { primaryIndexKey },
			new BooleanRowMapper());
	}

	public boolean doesSecondaryIndexKeyExist(SecondaryIndex secondaryIndex, Object secondaryIndexKey) throws SQLException {
		JdbcTemplate j = getJdbcTemplate();
		return j.query("select id from " + getSecondaryIndexTableName(secondaryIndex)
			+ " where id = ?",
			new Object[] { secondaryIndexKey },
			new TrueRowMapper()).size() == 1;
	}

	@SuppressWarnings("unchecked")
	public Integer getNodeIdOfSecondaryIndexKey(SecondaryIndex secondaryIndex, Object secondaryIndexKey)
	{
		JdbcTemplate j = getJdbcTemplate();
		return j.queryForInt("select p.node from " + getPrimaryIndexTableName() + " p"	
			+ " join " + getSecondaryIndexTableName(secondaryIndex) + " s on s.pkey = p.id"
			+ " where s.id =  ?",
			new Object[] { secondaryIndexKey });
	}
	@SuppressWarnings("unchecked")
	public Object getPrimaryIndexKeyOfSecondaryindexKey(SecondaryIndex secondaryIndex, Object secondaryIndexKey)
	{
		JdbcTemplate j = getJdbcTemplate();
		return j.queryForObject("select p.id from " + getPrimaryIndexTableName() + " p"	
			+ " join " + getSecondaryIndexTableName(secondaryIndex) + " s on s.pkey = p.id"
			+ " where s.id =  ?",
			new Object[] { secondaryIndexKey },
			new ObjectRowMapper(secondaryIndex.getResource().getPartitionDimension().getColumnType()));
	}
	
	@SuppressWarnings("unchecked")
	public Collection getSecondaryIndexKeysOfPrimaryIndexKey(SecondaryIndex secondaryIndex, Object primaryIndexKey)
	{
		JdbcTemplate j = getJdbcTemplate();
		String secondaryIndexTableName = getSecondaryIndexTableName(secondaryIndex);
		return j.query("select s.id from " + getPrimaryIndexTableName() + " p"	
			+ " join " + secondaryIndexTableName + " s on s.pkey = p.id"
			+ " where p.id = ?",
			new Object[] { primaryIndexKey },
			new ObjectRowMapper(secondaryIndex.getColumnInfo().getColumnType()));
	}
	
	public class IntRowMapper implements RowMapper {
		public Object mapRow(ResultSet rs, int rowNumber) throws SQLException {
			return rs.getInt(1);		
		}
	}
	public class BooleanRowMapper implements RowMapper {
		public Object mapRow(ResultSet rs, int rowNumber) throws SQLException {
			return rs.getBoolean(1);		
		}
	}
	public class ObjectRowMapper implements RowMapper {
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
	
}
