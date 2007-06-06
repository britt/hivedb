package org.hivedb.meta.persistence;

import org.apache.commons.dbcp.BasicDataSource;

public class HiveBasicDataSource extends BasicDataSource {
	public static final String CONNECTION_POOL_SIZE = "HiveDataSourceConnectionPoolSize";
	public static final int DEFAULT_POOL_SIZE = 32;

	public HiveBasicDataSource(String hiveUri, int poolSize){
		super();
		setUrl(hiveUri);	
		setMaxActive(poolSize);
	}
	
	public HiveBasicDataSource(String hiveUri)
	{
		this(hiveUri, getDefaultPoolSize());	
	}
	
	private static Integer getDefaultPoolSize() {
		try {
			String poolSize = System.getProperty(CONNECTION_POOL_SIZE);
			return Integer.parseInt(poolSize);
		} catch (Exception e) {
			return DEFAULT_POOL_SIZE;
		}
	}
}
