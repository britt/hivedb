package org.hivedb.meta.persistence;

import javax.sql.DataSource;
import org.hivedb.meta.Node;

public interface DataSourceProvider {
	public DataSource getDataSource(String uri);
	public DataSource getDataSource(Node node);
	public long getTimeout();
	public void setTimeout(long timeout);
}
