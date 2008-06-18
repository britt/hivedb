/**
 * 
 */
package org.hivedb.meta.persistence;

import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.hivedb.meta.Node;
import org.springframework.jdbc.datasource.LazyConnectionDataSourceProxy;

public class CachingDataSourceProvider implements DataSourceProvider {
	
	private static CachingDataSourceProvider INSTANCE = new CachingDataSourceProvider();
	
	private Map<String, LazyConnectionDataSourceProxy> cache = new HashMap<String, LazyConnectionDataSourceProxy>();
	//private Map<String, DataSource> cache = new HashMap<String, DataSource>();
	
	private CachingDataSourceProvider() {
	}
	
	public DataSource getDataSource(Node node) {
		return getDataSource(node.getUri());
	}

	public DataSource getDataSource(String uri) {
		LazyConnectionDataSourceProxy ds = cache.get(uri);
		//DataSource ds = cache.get(uri);
		if (ds == null) {
			ds = new LazyConnectionDataSourceProxy(new HiveBasicDataSource(uri));
			//ds = new HiveBasicDataSource(uri);
			cache.put(uri, ds);
		}
		return ds;
	}
	
	public static CachingDataSourceProvider getInstance() {
		return INSTANCE;
	}
}
