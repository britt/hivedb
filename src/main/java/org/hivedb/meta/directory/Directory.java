package org.hivedb.meta.directory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collection;

import javax.sql.DataSource;

import org.hivedb.DirectoryCorruptionException;
import org.hivedb.HiveKeyNotFoundException;
import org.hivedb.meta.KeySemaphore;
import org.hivedb.meta.Node;
import org.hivedb.meta.PartitionDimension;
import org.hivedb.meta.Resource;
import org.hivedb.meta.SecondaryIndex;
import org.hivedb.meta.persistence.HiveBasicDataSource;
import org.hivedb.util.QuickCache;
import org.hivedb.util.database.JdbcTypeMapper;
import org.hivedb.util.database.RowMappers;
import org.hivedb.util.database.Statements;
import org.hivedb.util.functional.Atom;
import org.hivedb.util.functional.Delay;
import org.hivedb.util.functional.Unary;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.PreparedStatementCreatorFactory;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.ParameterizedRowMapper;
import org.springframework.jdbc.core.simple.SimpleJdbcDaoSupport;

public class Directory extends SimpleJdbcDaoSupport implements NodeResolver, IndexWriter {
	private PartitionDimension partitionDimension;
	private IndexSqlFormatter sql = new IndexSqlFormatter();

	
	public Directory(PartitionDimension dimension, DataSource dataSource) {
		this.partitionDimension = dimension;
		this.setDataSource(dataSource);
	}
	
	public Directory(PartitionDimension dimension) {
		this.partitionDimension = dimension;
		this.setDataSource(new HiveBasicDataSource(dimension.getIndexUri()));
	}
	
	public PartitionDimension getPartitionDimension() {
		return this.partitionDimension;
	}
	
	public void insertPrimaryIndexKey(Node node, Object primaryIndexKey) {
		int[] types = new int[]{JdbcTypeMapper.primitiveTypeToJdbcType(primaryIndexKey.getClass()), Types.INTEGER};
		Object[] parameters = new Object[] {primaryIndexKey,node.getId() };
		doUpdate(sql.insertPrimaryIndexKey(partitionDimension), types, parameters);
	}

	public void insertSecondaryIndexKey(SecondaryIndex secondaryIndex, Object secondaryIndexKey, Object primaryindexKey) {
		Object[] parameters = new Object[] {secondaryIndexKey, primaryindexKey};
		int[] types = new int[]{secondaryIndex.getColumnInfo().getColumnType(), secondaryIndex.getResource().getColumnType()};
		doUpdate(sql.insertSecondaryIndexKey(secondaryIndex), types, parameters);
	}
	
	private static QuickCache cache = new QuickCache();

	public void updatePrimaryIndexKeyReadOnly(Object primaryIndexKey, boolean isReadOnly) {
		Object[] parameters = new Object[] {isReadOnly,primaryIndexKey};
		int[] types = new int[]{Types.BOOLEAN, JdbcTypeMapper.primitiveTypeToJdbcType(primaryIndexKey.getClass())};
		doUpdate(sql.updateReadOnlyOfPrimaryIndexKey(partitionDimension), types, parameters);
	}
	
	public void updatePrimaryIndexKeyOfResourceId(Resource resource, Object resourceId, Object newPrimaryIndexKey) {
		Object[] parameters = new Object[] {
			newPrimaryIndexKey,
			resourceId
        };
		int[] types = new int[]{
			JdbcTypeMapper.primitiveTypeToJdbcType(newPrimaryIndexKey.getClass()),
			resource.getColumnType()
        };
		doUpdate(
			sql.updateResourceId(resource),
			types,
			parameters);
	}
	
	public void deletePrimaryIndexKey(Object primaryIndexKey) {
		doUpdate(
			sql.deletePrimaryIndexKey(partitionDimension), 
			new int[]{JdbcTypeMapper.primitiveTypeToJdbcType(primaryIndexKey.getClass())}, 
			new Object[] {primaryIndexKey});
	}

	
	public void deleteSecondaryIndexKey(SecondaryIndex secondaryIndex, Object secondaryIndexKey, Object resourceId) {
		Object[] parameters = new Object[] {
			secondaryIndexKey,
			resourceId
		};
		int[] types = new int[] {
			secondaryIndex.getColumnInfo().getColumnType(),
			JdbcTypeMapper.primitiveTypeToJdbcType(resourceId.getClass())	
		};
		doUpdate(sql.deleteSingleSecondaryIndexKey(secondaryIndex), types, parameters);
	}

	@SuppressWarnings("unchecked")
	public boolean doesPrimaryIndexKeyExist(Object primaryIndexKey) {
		Collection count = doRead(sql.checkExistenceOfPrimaryKey(partitionDimension), 
				new Object[] { primaryIndexKey }, 
				RowMappers.newTrueRowMapper());
		return count.size() > 0;
	}
	
	public Collection<KeySemaphore> getKeySemamphoresOfPrimaryIndexKey(Object primaryIndexKey) {
		return doRead(sql.selectKeySemaphoreOfPrimaryIndexKey(partitionDimension), 
				new Object[] { primaryIndexKey }, 
				new KeySemaphoreRowMapper());
	}
	
	public boolean doesResourceIdExist(Resource resource, Object resourceId) {
		if (resource.isPartitioningResource())
			return doesPrimaryIndexKeyExist(resourceId);
		Collection<Object> count = 
				doRead(
					sql.checkExistenceOfResourceIndexSql(resource.getIdIndex()), 
					new Object[] { resourceId }, 
					RowMappers.newTrueRowMapper());
		return count.size() > 0;
	}
	

	public boolean doesSecondaryIndexKeyExist(SecondaryIndex secondaryIndex, Object secondaryIndexKey, Object resourceId) {
		Collection<Object> count = doRead(
				sql.checkExistenceOfSecondaryIndexSql(secondaryIndex),
				new Object[] { secondaryIndexKey, resourceId },
				RowMappers.newTrueRowMapper());
		return count.size() > 0;
	}

	public Collection<KeySemaphore> getKeySemaphoresOfSecondaryIndexKey(SecondaryIndex secondaryIndex, Object secondaryIndexKey)
	{
		return doRead(
			sql.selectKeySemaphoresOfSecondaryIndexKey(secondaryIndex), 
			new Object[] {secondaryIndexKey}, 
			new KeySemaphoreRowMapper());
	}
	
	@SuppressWarnings("unchecked")
	public Collection<KeySemaphore> getKeySemaphoresOfResourceId(Resource resource, Object resourceId)
	{
		return (Collection<KeySemaphore>) (resource.isPartitioningResource()
		? getKeySemamphoresOfPrimaryIndexKey(resourceId)
		: doRead(
			sql.selectKeySemaphoresOfResourceId(resource), 
			new Object[] {resourceId}, 
			new KeySemaphoreRowMapper()));
	}

	@SuppressWarnings("unchecked")
	public Collection<Object> getPrimaryIndexKeysOfSecondaryIndexKey(SecondaryIndex secondaryIndex, Object secondaryIndexKey)
	{
		return doRead(
			sql.selectPrimaryIndexKeysOfSecondaryIndexKey(secondaryIndex), 
			new Object[] {secondaryIndexKey}, 
			RowMappers.newObjectRowMapper(secondaryIndex.getResource().getPartitionDimension().getColumnType()));
	}

	@SuppressWarnings("unchecked")
	public Collection<Object> getSecondaryIndexKeysOfPrimaryIndexKey(SecondaryIndex secondaryIndex, Object primaryIndexKey)
	{
		return doRead(
			sql.selectSecondaryIndexKeysOfPrimaryKey(secondaryIndex),
			new Object[] { primaryIndexKey }, 
			RowMappers.newObjectRowMapper(secondaryIndex.getColumnInfo().getColumnType()));
	}

	public void deleteResourceId(Resource resource, Object id) {
		doUpdate(sql.deleteResourceId(resource), new int[] {resource.getColumnType()}, new Object[] {id});
	}

	@SuppressWarnings("unchecked")
	public Collection getResourceIdsOfSecondaryIndexKey(SecondaryIndex secondaryIndex, Object secondaryIndexKey) {
		return doRead(
				sql.selectResourceIdsOfSecondaryIndexKey(secondaryIndex),
				new Object[] { secondaryIndexKey }, 
				RowMappers.newObjectRowMapper(secondaryIndex.getResource().getColumnType()));
	}

	@SuppressWarnings("unchecked")
	public Collection getResourceIdsOfPrimaryIndexKey(Resource resource, Object primaryIndexKey) {
		return doRead(
				sql.selectResourceIdsOfPrimaryIndexKey(resource.getIdIndex()),
				new Object[] { primaryIndexKey }, 
				RowMappers.newObjectRowMapper(resource.getColumnType()));
	}
	
	@SuppressWarnings("unchecked")
	public class KeySemaphoreRowMapper implements ParameterizedRowMapper {
		public Object mapRow(ResultSet rs, int arg1) throws SQLException {
			return new KeySemaphore(rs.getInt("node"), rs.getBoolean("read_only"));
		}	
	}

	@SuppressWarnings("unchecked")
	public Collection getSecondaryIndexKeysOfResourceId(SecondaryIndex secondaryIndex, Object id) {
		return doRead(
				sql.selectSecondaryIndexKeyOfResourceId(secondaryIndex), 
				new Object[] { id }, 
				RowMappers.newObjectRowMapper(secondaryIndex.getColumnInfo().getColumnType()));
	}
	
	@SuppressWarnings("unchecked")
	private<T> Collection<T> doRead(String sql, Object[] parameters, RowMapper mapper) {
		try{
			return (Collection<T>) getJdbcTemplate().query(sql,	parameters, mapper);
		} catch(EmptyResultDataAccessException e) {
			throw new HiveKeyNotFoundException(String.format("Unable to get secondary index keys of primary index key %s, key not found.", parameters[0]), parameters[0],e);
		}
	}
	
	private void doUpdate(String sql, int[] types, Object[] parameters){
		PreparedStatementCreatorFactory factory = 
			Statements.newStmtCreatorFactory(sql, types);
		getJdbcTemplate().update(factory.newPreparedStatementCreator(parameters));
	}

	public void insertResourceId(Resource resource, Object id, Object primaryIndexKey) {
		doUpdate(
				sql.insertResourceId(resource),
				new int[] {resource.getColumnType(),resource.getPartitionDimension().getColumnType()}, 
				new Object[]{id,primaryIndexKey});
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
	
	public Unary<KeySemaphore,Integer> semaphoreToId() {
		return new Unary<KeySemaphore, Integer>(){

			public Integer f(KeySemaphore item) {
				return item.getId();
			}};
	}
	
	public Unary<KeySemaphore, Boolean> semaphoreToReadOnly() {
		return new Unary<KeySemaphore, Boolean>(){

			public Boolean f(KeySemaphore item) {
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
