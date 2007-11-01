package org.hivedb.meta.directory;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;

import org.hivedb.meta.Resource;
import org.hivedb.meta.SecondaryIndex;
import org.hivedb.util.QuickCache;
import org.hivedb.util.database.Statements;
import org.hivedb.util.functional.Delay;
import org.hivedb.util.functional.Transform;
import org.hivedb.util.functional.Unary;
import org.springframework.jdbc.core.PreparedStatementCreatorFactory;
import org.springframework.jdbc.core.simple.SimpleJdbcDaoSupport;
import org.springframework.jdbc.core.support.JdbcDaoSupport;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

/***
 * A class for transactionally performing batches of directory operations.
 * @author bcrawford
 *
 */
public class BatchIndexWriter extends SimpleJdbcDaoSupport {
	private Directory directory;
	private IndexSqlFormatter sql;
	
	public BatchIndexWriter(Directory directory) {
		this.directory = directory;
		this.sql = new IndexSqlFormatter();
		this.setDataSource(directory.getDataSource());
	}
		
	public Integer insertSecondaryIndexKeys(final Map<SecondaryIndex, Collection<Object>> secondaryIndexValueMap, final Object resourceId) {
		return (Integer) directory.newTransaction().execute(new TransactionCallback() {
			public Integer doInTransaction(TransactionStatus status) {	
				return Transform.flatMap(new Unary<Map.Entry<SecondaryIndex, Collection<Object>>, Collection<Object>>() {
					public Collection<Object> f(final Entry<SecondaryIndex, Collection<Object>> secondaryIndexKeysEntry) {
						return Transform.map(new Unary<Object, Object>() { 
							 public Object f(Object secondaryIndexKey) {
								 directory.insertSecondaryIndexKey(
											secondaryIndexKeysEntry.getKey(),
											secondaryIndexKey,
											resourceId);
								 return secondaryIndexKey;
							}}, secondaryIndexKeysEntry.getValue());
					}},
					secondaryIndexValueMap.entrySet()).size();
			}
		});
	}
	
	public Integer deleteSecondaryIndexKeys(final Map<SecondaryIndex, Collection<Object>> secondaryIndexValueMap, final Object resourceId) {
		return (Integer) directory.newTransaction().execute(new TransactionCallback() {
			public Integer doInTransaction(TransactionStatus status) {	
				return Transform.flatMap(new Unary<Map.Entry<SecondaryIndex, Collection<Object>>, Collection<Object>>() {
					public Collection<Object> f(final Entry<SecondaryIndex, Collection<Object>> secondaryIndexKeysEntry) {
						return Transform.map(new Unary<Object, Object>() { 
							 public Object f(Object secondaryIndexKey) {
								 directory.deleteSecondaryIndexKey(secondaryIndexKeysEntry.getKey(), secondaryIndexKey, resourceId);
								 return secondaryIndexKey;
							}}, secondaryIndexKeysEntry.getValue());
					}},
					secondaryIndexValueMap.entrySet()).size();
			}
		});
	}
	
	public Integer deleteAllSecondaryIndexKeysOfResourceId(final Resource resource, Object id) {
		final Object[] parameters = new Object[] {id};
		final PreparedStatementCreatorFactory deleteFactory = 
			Statements.newStmtCreatorFactory(sql.deleteAllSecondaryIndexKeysForResourceId(resource.getIdIndex()), resource.getColumnType());
		
		return (Integer) directory.newTransaction().execute(new TransactionCallback(){
			public Object doInTransaction(TransactionStatus arg0) {
				Integer rowsAffected = 0;
				for(SecondaryIndex secondaryIndex : resource.getSecondaryIndexes()){
					deleteFactory.setSqlToUse(sql.deleteAllSecondaryIndexKeysForResourceId(secondaryIndex));
					rowsAffected += getJdbcTemplate().update(deleteFactory.newPreparedStatementCreator(parameters));
				}
				return rowsAffected;
			}});
	}
}
