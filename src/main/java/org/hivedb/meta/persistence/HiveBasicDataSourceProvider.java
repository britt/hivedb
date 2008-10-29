/**
 *
 */
package org.hivedb.meta.persistence;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hivedb.HiveRuntimeException;
import org.hivedb.meta.Node;
import org.springframework.jdbc.datasource.LazyConnectionDataSourceProxy;

import javax.sql.DataSource;
import java.util.Collection;
import java.util.HashSet;

public class HiveBasicDataSourceProvider implements HiveDataSourceProvider {
  private Log log = LogFactory.getLog(HiveBasicDataSource.class);

  private Collection<HiveBasicDataSource> dataSourcesToClose;

  public HiveBasicDataSourceProvider() {
    dataSourcesToClose = new HashSet<HiveBasicDataSource>();
  }

  public DataSource getDataSource(Node node) {
    return getDataSource(node.getUri());
  }

  public void close() {
    HiveRuntimeException exceptionWhileClosing = null;
    for (HiveBasicDataSource dataSource : dataSourcesToClose) {
      try {
        dataSource.close();
      } catch (Exception e) {
        exceptionWhileClosing = new HiveRuntimeException("Error closing datasources. Possibly more than one cause.", e);
      }
    }
    if (exceptionWhileClosing != null) {
      throw exceptionWhileClosing;
    }
  }

  public DataSource getDataSource(String uri) {
    HiveBasicDataSource ds = new HiveBasicDataSource(uri);
    LazyConnectionDataSourceProxy dataSourceProxy = new LazyConnectionDataSourceProxy(ds);
    dataSourcesToClose.add(ds);
    return dataSourceProxy;
  }
}