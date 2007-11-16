package org.hivedb.meta.persistence;

import javax.sql.DataSource;

import org.hivedb.HiveRuntimeException;
import org.hivedb.meta.Node;

public class CloningDataSourceProvider implements DataSourceProvider {
	private HiveBasicDataSource prototype;
	
	public CloningDataSourceProvider(HiveBasicDataSource prototype) {
		this.prototype = prototype;
	}
	
	public DataSource getDataSource(String uri) {
		HiveBasicDataSource clone;
		try {
			clone = prototype.clone();
		} catch (CloneNotSupportedException e) {
			throw new HiveRuntimeException("You must provide a cloneable prototype.",e);
		}
		clone.setUrl(uri);
		return clone;
	}

	public DataSource getDataSource(Node node) {
		return getDataSource(node.getUri());
	}

}
