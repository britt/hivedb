package org.hivedb.meta.persistence;

import java.sql.SQLException;

import org.apache.commons.dbcp.BasicDataSource;

public class HiveBasicDataSource extends BasicDataSource {
	public static final String CONNECTION_POOL_SIZE = "HiveDataSourceConnectionPoolSize";
	public static final int DEFAULT_POOL_SIZE = 32;
	private Long connectionTimeout = 500l;
	private Long socketTimeout  = 500l;
	
	public HiveBasicDataSource() {}
	
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
	
	@Override
	public HiveBasicDataSource clone() throws CloneNotSupportedException {
		HiveBasicDataSource clone = new HiveBasicDataSource();
		try {
			clone.setLoginTimeout(this.getLoginTimeout());
		} catch (SQLException e) {
			//Quash
		}
		clone.setConnectionTimeout(this.getConnectionTimeout());
		clone.setSocketTimeout(this.getSocketTimeout());
		clone.setInitialSize(this.getInitialSize());
		clone.setMinIdle(this.getMinIdle());
		clone.setMaxIdle(this.getMaxIdle());
		clone.setMaxActive(this.getMaxActive());
		clone.setMaxWait(this.getMaxWait());
		clone.setMaxOpenPreparedStatements(this.getMaxOpenPreparedStatements());
		clone.setDefaultTransactionIsolation(this.getDefaultTransactionIsolation());
		clone.setDefaultAutoCommit(this.getDefaultAutoCommit());
		clone.setPoolPreparedStatements(this.isPoolPreparedStatements());
		clone.setMinEvictableIdleTimeMillis(this.getMinEvictableIdleTimeMillis());
		return clone;
	}

	public Long getConnectionTimeout() {
		return connectionTimeout;
	}

	public void setConnectionTimeout(Long connectionTimeout) {
		this.connectionTimeout = connectionTimeout;
		this.addConnectionProperty("connectTimeout", connectionTimeout.toString());
	}

	public Long getSocketTimeout() {
		return socketTimeout;
	}

	public void setSocketTimeout(Long socketTimeout) {
		this.socketTimeout = socketTimeout;
		this.addConnectionProperty("socketTimeout", socketTimeout.toString());
	}

}
