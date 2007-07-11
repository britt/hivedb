/**
 * 
 */
package org.hivedb.meta.persistence;

import javax.sql.DataSource;

import org.hivedb.meta.Node;

public class HiveBasicDataSourceProvider implements DataSourceProvider {
	private Long timeoutInMillis = 0l;
	
	public HiveBasicDataSourceProvider(long timeout) {
		this.timeoutInMillis = timeout;
	}
	
	public DataSource getDataSource(Node node) {
		return getDataSource(node.getUri());
	}

	public long getTimeout() {
		return timeoutInMillis;
	}

	public void setTimeout(long timeout) {
		this.timeoutInMillis = timeout;
	}

	public DataSource getDataSource(String uri) {
		HiveBasicDataSource ds = new HiveBasicDataSource(uri);
		ds.addConnectionProperty("socketTimeout", timeoutInMillis.toString());
		ds.addConnectionProperty("connectTimeout", timeoutInMillis.toString());
		return ds;
	}
	
}