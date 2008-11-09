package org.hivedb.directory;

import org.hivedb.Resource;
import org.hivedb.SecondaryIndex;
import org.hivedb.util.database.Statements;
import org.hivedb.util.functional.Transform;
import org.hivedb.util.functional.Unary;
import org.springframework.jdbc.core.PreparedStatementCreatorFactory;
import org.springframework.jdbc.core.simple.SimpleJdbcDaoSupport;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;

/**
 * A class for transactionally performing batches of directory operations. This class is a close collaborator with DbDirectory.
 * It is probably more appropriately and inner class of DbDirectory.  It is only extracted for readability.
 *
 * @author bcrawford
 */
public class BatchIndexWriter extends SimpleJdbcDaoSupport {
  private DbDirectory directory;
  private IndexSqlFormatter sql;

  public BatchIndexWriter(DbDirectory directory, IndexSqlFormatter indexSqlFormatter) {
    this.directory = directory;
    this.sql = indexSqlFormatter;
    this.setDataSource(directory.getDataSource());
  }

  public Integer insertSecondaryIndexKeys(final Map<SecondaryIndex, Collection<Object>> secondaryIndexValueMap, final Object resourceId) {
    return (Integer) directory.newTransaction().execute(new TransactionCallback() {
      public Integer doInTransaction(TransactionStatus status) {
        return Transform.flatMap(new Unary<Map.Entry<SecondaryIndex, Collection<Object>>, Collection<Object>>() {
          public Collection<Object> f(final Entry<SecondaryIndex, Collection<Object>> secondaryIndexKeysEntry) {
            return Transform.map(new Unary<Object, Object>() {
              public Object f(Object secondaryIndexKey) {
                return directory.insertSecondaryIndexKeyNoTransaction(secondaryIndexKeysEntry.getKey(), secondaryIndexKey, resourceId);
              }
            }, secondaryIndexKeysEntry.getValue());
          }
        },
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
                return directory.deleteSecondaryIndexKeyNoTransaction(secondaryIndexKeysEntry.getKey(), secondaryIndexKey, resourceId);
              }
            }, secondaryIndexKeysEntry.getValue());
          }
        },
          secondaryIndexValueMap.entrySet()).size();
      }
    });
  }

  public Integer deleteAllSecondaryIndexKeysOfResourceId(final Resource resource, Object id) {
    final Object[] parameters = new Object[]{id};

    return (Integer) directory.newTransaction().execute(new TransactionCallback() {
      public Object doInTransaction(TransactionStatus arg0) {
        Integer rowsAffected = 0;
        for (SecondaryIndex secondaryIndex : resource.getSecondaryIndexes()) {
          PreparedStatementCreatorFactory deleteIndexFactory =
            Statements.newStmtCreatorFactory(sql.deleteAllSecondaryIndexKeysForResourceId(secondaryIndex), resource.getColumnType());
          rowsAffected += getJdbcTemplate().update(deleteIndexFactory.newPreparedStatementCreator(parameters));
        }
        return rowsAffected;
      }
    });
  }
}
