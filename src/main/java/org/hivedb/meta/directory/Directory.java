package org.hivedb.meta.directory;

import org.hivedb.DirectoryCorruptionException;
import org.hivedb.HiveKeyNotFoundException;
import org.hivedb.Lockable.Status;
import org.hivedb.meta.*;
import org.hivedb.meta.persistence.CachingDataSourceProvider;
import org.hivedb.util.QuickCache;
import org.hivedb.util.database.JdbcTypeMapper;
import org.hivedb.util.database.RowMappers;
import org.hivedb.util.database.Schemas;
import org.hivedb.util.database.Statements;
import org.hivedb.util.functional.Atom;
import org.hivedb.util.functional.Delay;
import org.hivedb.util.functional.Unary;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.ParameterizedRowMapper;
import org.springframework.jdbc.core.simple.SimpleJdbcDaoSupport;
import org.springframework.jdbc.core.support.JdbcDaoSupport;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collection;

public class Directory extends SimpleJdbcDaoSupport implements NodeResolver, IndexWriter {
	private static QuickCache cache = new QuickCache();
	private PartitionDimension partitionDimension;
	private IndexSqlFormatter sql = new IndexSqlFormatter();

	
	public Directory(PartitionDimension dimension, DataSource dataSource) {
		this.partitionDimension = dimension;
		this.setDataSource(dataSource);
	}
	
	public Directory(PartitionDimension dimension) {
		this.partitionDimension = dimension;
		this.setDataSource(CachingDataSourceProvider.getInstance().getDataSource(dimension.getIndexUri()));
	}
	
	public PartitionDimension getPartitionDimension() {
		return this.partitionDimension;
	}
	
	public void insertPrimaryIndexKey(final Node node, final Object primaryIndexKey) {
		newTransaction().execute(new TransactionCallback(){
			public Object doInTransaction(TransactionStatus arg0) {
				int[] types = new int[]{JdbcTypeMapper.primitiveTypeToJdbcType(primaryIndexKey.getClass()), Types.INTEGER};
				Object[] parameters = new Object[] {primaryIndexKey,node.getId() };
				
				if(lockPrimaryKeyForInsert(partitionDimension, primaryIndexKey, node))
					doUpdate(sql.insertPrimaryIndexKey(partitionDimension), types, parameters);
				return primaryIndexKey;
			}
		});
	}

	public void insertSecondaryIndexKey(final SecondaryIndex secondaryIndex, final Object secondaryIndexKey, final Object resourceId) {
		newTransaction().execute(new TransactionCallback(){
			public Object doInTransaction(TransactionStatus arg0) {
				return insertSecondaryIndexKeyNoTransaction(secondaryIndex, secondaryIndexKey, resourceId);
			}
		});
	}
	
	Object insertSecondaryIndexKeyNoTransaction(final SecondaryIndex secondaryIndex, final Object secondaryIndexKey, final Object resourceId) {
		Object[] parameters = new Object[] {secondaryIndexKey, resourceId};
		int[] types = new int[]{secondaryIndex.getColumnInfo().getColumnType(), secondaryIndex.getResource().getColumnType()};
		
		if(lockSecondaryIndexKey(secondaryIndex, secondaryIndexKey, resourceId))
			doUpdate(sql.insertSecondaryIndexKey(secondaryIndex), types, parameters);
		return secondaryIndexKey;
	}

	public void updatePrimaryIndexKeyReadOnly(final Object primaryIndexKey, final boolean isReadOnly) {
		newTransaction().execute(new TransactionCallback(){
			public Object doInTransaction(TransactionStatus arg0) {
				Object[] parameters = new Object[] {isReadOnly,primaryIndexKey};
				int[] types = new int[]{Types.BOOLEAN, JdbcTypeMapper.primitiveTypeToJdbcType(primaryIndexKey.getClass())};
				lockPrimaryKeyForUpdate(partitionDimension, primaryIndexKey);
				doUpdate(sql.updateReadOnlyOfPrimaryIndexKey(partitionDimension), types, parameters);
				return primaryIndexKey;
			}
		});
	}
	
	public void updatePrimaryIndexKeyOfResourceId(final Resource resource, final Object resourceId, final Object newPrimaryIndexKey) {
		newTransaction().execute(new TransactionCallback(){
			public Object doInTransaction(TransactionStatus arg0) {
				Object[] parameters = new Object[] {newPrimaryIndexKey, resourceId };
				int[] types = new int[]{
						JdbcTypeMapper.primitiveTypeToJdbcType(newPrimaryIndexKey.getClass()),
						resource.getColumnType()
			        };
				lockResourceId(resource, resourceId);
				doUpdate(sql.updateResourceId(resource),types,parameters);
				return resourceId;
			}
		});
	}
	
	public void deletePrimaryIndexKey(final Object primaryIndexKey) {
		newTransaction().execute(new TransactionCallback(){
			public Object doInTransaction(TransactionStatus arg0) {
				lockPrimaryKeyForUpdate(partitionDimension, primaryIndexKey);
				doUpdate(
						sql.deletePrimaryIndexKey(partitionDimension), 
						new int[]{JdbcTypeMapper.primitiveTypeToJdbcType(primaryIndexKey.getClass())}, 
						new Object[] {primaryIndexKey});
				return primaryIndexKey;
			}
		});
	}

	
	public void deleteSecondaryIndexKey(final SecondaryIndex secondaryIndex, final Object secondaryIndexKey, final Object resourceId) {
		newTransaction().execute(new TransactionCallback(){
			public Object doInTransaction(TransactionStatus arg0) {
				return deleteSecondaryIndexKeyNoTransaction(secondaryIndex, secondaryIndexKey, resourceId);
			}
		});
	}
	
	Object deleteSecondaryIndexKeyNoTransaction(final SecondaryIndex secondaryIndex, final Object secondaryIndexKey, final Object resourceId) {
		Object[] parameters = new Object[] {secondaryIndexKey, resourceId };
		int[] types = new int[] {
			secondaryIndex.getColumnInfo().getColumnType(),
			JdbcTypeMapper.primitiveTypeToJdbcType(resourceId.getClass())	
		};
		lockSecondaryIndexKey(secondaryIndex, secondaryIndexKey, resourceId);
		doUpdate(sql.deleteSingleSecondaryIndexKey(secondaryIndex), types, parameters);
		return secondaryIndexKey;
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
	
	public Collection<Object> getPrimaryIndexKeysOfSecondaryIndexKey(SecondaryIndex secondaryIndex, Object secondaryIndexKey)
	{
		return doRead(
			sql.selectPrimaryIndexKeysOfSecondaryIndexKey(secondaryIndex), 
			new Object[] {secondaryIndexKey}, 
			RowMappers.newObjectRowMapper(secondaryIndex.getResource().getPartitionDimension().getColumnType()));
	}
	
	public Collection<Object> getSecondaryIndexKeysOfPrimaryIndexKey(SecondaryIndex secondaryIndex, Object primaryIndexKey)
	{
		return doRead(
			sql.selectSecondaryIndexKeysOfPrimaryKey(secondaryIndex),
			new Object[] { primaryIndexKey }, 
			RowMappers.newObjectRowMapper(secondaryIndex.getColumnInfo().getColumnType()));
	}

	public void deleteResourceId(final Resource resource, final Object id) {
		newTransaction().execute(new TransactionCallback(){
			public Object doInTransaction(TransactionStatus arg0) {
				lockResourceId(resource, id);
				doUpdate(sql.deleteResourceId(resource), new int[] {resource.getColumnType()}, new Object[] {id});
				return id;
			}
		});
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
			return new KeySemaphore(rs.getInt("node"), resolveStatus(rs));
		}

		private Status resolveStatus(ResultSet rs) throws SQLException {
			return Status.getByValue(rs.getInt("status"));
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
			throw new HiveKeyNotFoundException(String.format("Directory query returned no results. %s with parameters: %s", sql, parameters), e);
		}
	}
	
	private void doUpdate(String sql, int[] types, Object[] parameters){
		getJdbcTemplate().update(Statements.newStmtCreatorFactory(sql, types).newPreparedStatementCreator(parameters));
	}

	public void insertResourceId(final Resource resource, final Object id, final Object primaryIndexKey) {
		newTransaction().execute(new TransactionCallback(){
			public Object doInTransaction(TransactionStatus arg0) {
				if(lockResourceId(resource, id))
					doUpdate(sql.insertResourceId(resource),
						new int[] {resource.getColumnType(),resource.getPartitionDimension().getColumnType()}, 
						new Object[]{id,primaryIndexKey});
				return id;
			}
		});
	}

	@SuppressWarnings("unchecked")
	public Object getPrimaryIndexKeyOfResourceId(Resource resource, Object id) {
		Collection keys = doRead(
				sql.selectPrimaryIndexKeysOfResourceId(resource), 
				new Object[] {id}, 
				RowMappers.newObjectRowMapper(resource.getPartitionDimension().getColumnType()));
		if( keys.size() == 0)
			throw new HiveKeyNotFoundException(String.format("Unable to find primary key for resource %s with id %s", resource.getName(), id), id);
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
				return !item.getStatus().equals(Status.writable);
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
	
	private void setTransactionManager(TransactionTemplate transactionTemplate, final JdbcDaoSupport jdbcDaoSupport) {
		transactionTemplate.setTransactionManager( (DataSourceTransactionManager) cache.get(jdbcDaoSupport.getDataSource(), new Delay<DataSourceTransactionManager>() {
			public DataSourceTransactionManager f() {
				return new DataSourceTransactionManager(jdbcDaoSupport.getDataSource());
			}	
		}));
	}
	
	private boolean lockPrimaryKeyForInsert(PartitionDimension dimension, Object primaryIndexKey, Node node) {
		return doRead(sql.selectCompositeKeyForUpdateLock(Schemas.getPrimaryIndexTableName(partitionDimension), "id", "node"), 
				new Object[]{primaryIndexKey, node.getId()},
				RowMappers.newTrueRowMapper()).size() == 0;
	}
	
	private boolean lockPrimaryKeyForUpdate(PartitionDimension dimension, Object primaryIndexKey) {
		return doRead(sql.selectForUpdateLock(Schemas.getPrimaryIndexTableName(partitionDimension), "id"), 
				new Object[]{primaryIndexKey},
				RowMappers.newTrueRowMapper()).size() == 0;
	}
	
	private boolean lockSecondaryIndexKey(SecondaryIndex secondaryIndex, Object secondaryIndexKey, Object resourceId){
		return doRead(sql.selectCompositeKeyForUpdateLock(Schemas.getSecondaryIndexTableName(secondaryIndex), "id", "pkey"), 
				new Object[]{secondaryIndexKey, resourceId},
				RowMappers.newTrueRowMapper()).size() == 0;
	}
	
	private boolean lockResourceId(Resource resource, Object resourceId) {
		return doRead(sql.selectForUpdateLock(Schemas.getResourceIndexTableName(resource), "id"),
				new Object[]{resourceId},
				RowMappers.newTrueRowMapper()).size() == 0;
	}

	public TransactionTemplate newTransaction() {
		TransactionTemplate t = new TransactionTemplate();
		setTransactionManager(t, this);
		return t;
	}
}
