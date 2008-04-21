package org.hivedb.meta.persistence;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hivedb.util.HiveUtils;

import com.mchange.v2.c3p0.ComboPooledDataSource;

public class HiveBasicDataSource implements DataSource, Cloneable {
	private Log log = LogFactory.getLog(HiveBasicDataSource.class);
	public static final String CONNECTION_POOL_SIZE = "HiveDataSourceConnectionPoolSize";
	public static final int DEFAULT_POOL_SIZE = 32;
	private ComboPooledDataSource comboPooledDataSource;
	
	public HiveBasicDataSource() {
		comboPooledDataSource = new ComboPooledDataSource();
	}
	
	public HiveBasicDataSource(String hiveUri, int poolSize){
		comboPooledDataSource = new ComboPooledDataSource();
		comboPooledDataSource.setJdbcUrl(hiveUri);
		comboPooledDataSource.setTestConnectionOnCheckout(true);
		comboPooledDataSource.setMaxPoolSize(getDefaultPoolSize());
		comboPooledDataSource.setMaxIdleTime(3600);
		log.debug(String.format("HiveBasicDataSource created for %s.  Max pool size: %s", this.getUrl(), this.getMaxActive()));
	}
	
	public HiveBasicDataSource(String hiveUri) {
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
	
	public String getUrl() { //publicize for testing
		return comboPooledDataSource.getJdbcUrl();
	}
	
	public void setUrl(String url) {
		comboPooledDataSource.setJdbcUrl(url);
	}
	
	@Override
	public HiveBasicDataSource clone() throws CloneNotSupportedException {
		HiveBasicDataSource clone = new HiveBasicDataSource();
		clone.setMaxActive(this.getMaxActive());
		clone.setPassword(this.getPassword());
		clone.setUrl(this.getUrl());
		clone.setUsername(this.getUsername());
		clone.setValidationQuery(this.getValidationQuery());
		return clone;
	}

	@Override
	public int hashCode() {
		return HiveUtils.makeHashCode(
				this.getMaxActive(),
				this.getPassword(),
				this.getUrl(),
				this.getUsername(),
				this.getValidationQuery()
		);
	}
	
	public Connection getConnection() throws SQLException {
		Connection connection = comboPooledDataSource.getConnection();
		log.debug("Loaned connection, current active connections: " + this.getNumActive());
		return connection;
	}
	
	public Connection getConnection(String username, String password)
			throws SQLException {
		Connection connection = comboPooledDataSource.getConnection(username,password);		
		log.debug("Loaned connection, current active connections: " + this.getNumActive());
		return connection;
	}

	public PrintWriter getLogWriter() throws SQLException {
		return comboPooledDataSource.getLogWriter();
	}

	public int getLoginTimeout() throws SQLException {
		return comboPooledDataSource.getLoginTimeout();
	}

	public void setLogWriter(PrintWriter out) throws SQLException {
		comboPooledDataSource.setLogWriter(out);
	}

	public void setLoginTimeout(int seconds) throws SQLException {
		comboPooledDataSource.setLoginTimeout(seconds);
	}
	
	public int getMaxActive() {
		return comboPooledDataSource.getMaxPoolSize();
	}
	
	public void setMaxActive(int maxActive) {
		comboPooledDataSource.setMaxPoolSize(maxActive);
	}
	
	private int getNumActive() throws SQLException {
		return comboPooledDataSource.getNumBusyConnections();
	}
	
	public String getUsername() {
		return comboPooledDataSource.getUser();
	}
	
	public void setUsername(String username) {
		comboPooledDataSource.setUser(username);
	}
	
	public String getPassword() {
		return comboPooledDataSource.getPassword();
	}
	
	public void setPassword(String password) {
		comboPooledDataSource.setPassword(password);
	}
	
	public String getValidationQuery() {
		return comboPooledDataSource.getPreferredTestQuery();
	}
	
	public void setValidationQuery(String validationQuery) {
		comboPooledDataSource.setPreferredTestQuery(validationQuery);
	}

  public boolean isWrapperFor(Class<?> iface) {
    return false;
  }

  public Object unwrap(Class<Object> iface) { throw new UnsupportedOperationException();}
}
