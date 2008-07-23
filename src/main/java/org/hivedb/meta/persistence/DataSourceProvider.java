package org.hivedb.meta.persistence;

import org.hivedb.meta.Node;

import javax.sql.DataSource;

public interface DataSourceProvider {
	public DataSource getDataSource(String uri);
	public DataSource getDataSource(Node node);
}
