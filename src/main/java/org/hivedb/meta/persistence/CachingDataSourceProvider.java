/**
 *
 */
package org.hivedb.meta.persistence;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hivedb.meta.Node;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

public class CachingDataSourceProvider implements HiveDataSourceProvider {
  private static final Log log = LogFactory.getLog(CachingDataSourceProvider.class);

  private static CachingDataSourceProvider INSTANCE = new CachingDataSourceProvider();

  private Map<String, DataSource> cache = new HashMap<String, DataSource>();

  private HiveBasicDataSourceProvider delegate;

  private CachingDataSourceProvider() {
    this.delegate = new HiveBasicDataSourceProvider();
  }

  public DataSource getDataSource(Node node) {
    return getDataSource(node.getUri());
  }

  public void close() {
    delegate.close();
  }

  public DataSource getDataSource(String uri) {
    DataSource ds = cache.get(uri);
    if (ds == null) {
      ds = delegate.getDataSource(uri);
      cache.put(uri, ds);
    }
    return ds;
  }

  public static CachingDataSourceProvider getInstance() {
    return INSTANCE;
  }
}
