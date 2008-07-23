/**
 * 
 */
package org.hivedb.meta.persistence;

import org.hivedb.meta.Node;
import org.springframework.jdbc.datasource.LazyConnectionDataSourceProxy;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

public class CachingDataSourceProvider implements DataSourceProvider {
	
	private static CachingDataSourceProvider INSTANCE = new CachingDataSourceProvider();
	
	private Map<String, LazyConnectionDataSourceProxy> cache = new HashMap<String, LazyConnectionDataSourceProxy>();
	
	private CachingDataSourceProvider() {
	}
	
	public DataSource getDataSource(Node node) {
		return getDataSource(node.getUri());
	}

	public DataSource getDataSource(String uri) {
		LazyConnectionDataSourceProxy ds = cache.get(uri);
		if (ds == null) {
			ds = new LazyConnectionDataSourceProxy(new HiveBasicDataSource(uri));
			cache.put(uri, ds);
		}
		return ds;
	}
	
	public static CachingDataSourceProvider getInstance() {
		return INSTANCE;
	}
}
