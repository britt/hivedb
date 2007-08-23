package org.hivedb.meta.directory;

import static org.hivedb.management.statistics.DirectoryPerformanceStatisticsMBean.SECONDARY_INDEX_DELETE;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;

import org.hivedb.meta.Resource;
import org.hivedb.meta.SecondaryIndex;
import org.hivedb.util.Proxies;
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
	private static QuickCache cache = new QuickCache();
	private Directory directory;
	private IndexSqlFormatter sql;
	
	public BatchIndexWriter(Directory directory) {
		this.directory = directory;
		this.sql = new IndexSqlFormatter();
		this.setDataSource(directory.getDataSource());
	}
	
	private void setTransactionManager(TransactionTemplate transactionTemplate, final JdbcDaoSupport jdbcDaoSupport) {
		transactionTemplate.setTransactionManager( (DataSourceTransactionManager) cache.get(jdbcDaoSupport.getDataSource(), new Delay<DataSourceTransactionManager>() {
			public DataSourceTransactionManager f() {
				return new DataSourceTransactionManager(jdbcDaoSupport.getDataSource());
			}	
		}));
	}
	
	public Integer insertSecondaryIndexKeys(final Map<SecondaryIndex, Collection<Object>> secondaryIndexValueMap, final Object resourceId) {
		TransactionTemplate transactionTemplate = new TransactionTemplate();
		setTransactionManager(transactionTemplate, this);
		
		return (Integer) transactionTemplate.execute(new TransactionCallback() {
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
		TransactionTemplate transactionTemplate = new TransactionTemplate();
		setTransactionManager(transactionTemplate, this);
		
		return (Integer) transactionTemplate.execute(new TransactionCallback() {
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
		
		TransactionTemplate transactionTemplate = new TransactionTemplate();
		setTransactionManager(transactionTemplate, this);
		
		return (Integer) transactionTemplate.execute(new TransactionCallback(){
			public Object doInTransaction(TransactionStatus arg0) {
				Integer rowsAffected = 0;
				for(SecondaryIndex secondaryIndex : resource.getSecondaryIndexes()){
					deleteFactory.setSqlToUse(sql.deleteAllSecondaryIndexKeysForResourceId(secondaryIndex));
					rowsAffected += Proxies.newJdbcUpdateProxy(directory.getPerformanceStatistics(), SECONDARY_INDEX_DELETE, parameters, deleteFactory, getJdbcTemplate()).execute();
				}
				return rowsAffected;
			}}
		);
	}

}
