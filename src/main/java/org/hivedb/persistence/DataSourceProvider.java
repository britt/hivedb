package org.hivedb.persistence;

import javax.sql.DataSource;

public interface DataSourceProvider {
	public DataSource getDataSource(String uri);
}
