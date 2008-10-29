package org.hivedb.persistence;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hivedb.util.HiveUtils;

import com.mchange.v2.c3p0.ComboPooledDataSource;

public class HiveBasicDataSource implements DataSource, Cloneable {
	private Log log = LogFactory.getLog(HiveBasicDataSource.class);
	private ComboPooledDataSource comboPooledDataSource;
	                   
	public HiveBasicDataSource() {
		comboPooledDataSource = new ComboPooledDataSource();
	}
	
	public HiveBasicDataSource(String hiveUri) {
		this();
		comboPooledDataSource.setJdbcUrl(hiveUri);
		log.debug(String.format("HiveBasicDataSource created: %s", comboPooledDataSource.toString()));
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

    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean isWrapperFor(Class<?> iface) {
    return false;
  }
}
