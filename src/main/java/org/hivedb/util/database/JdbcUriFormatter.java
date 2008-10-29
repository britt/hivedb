package org.hivedb.util.database;

import static org.hivedb.util.HiveUtils.empty;

import java.util.Map;
import java.util.Properties;

import org.hivedb.Node;
import org.hivedb.util.FilteringStringBuilder;


public class JdbcUriFormatter {
	private static final String OPTIONS = "options";
	private static final String PASSWORD = "password";
	private static final String USERNAME = "username";
	private static final String PORT = "port";
	private static final String DATABASE = "database";
	private static final String HOST = "host";
	private static final String DRIVER = "driver";
	
	private Properties p;
	
	public JdbcUriFormatter(Node node) {
		this.p = getConnectionPropertiesFromNode(node);
	}
	
	public String getUri() {
		return new UriBuilder(p)
				.append("jdbc:")
				.filter("get", DRIVER).append(":")
				.chain(HOST).add("get").add("format", "//%s").end()
				.chain(PORT).add("get").add("format", ":%s").end()
				.append(p.containsKey(HOST) ? "/" : "")
				.filter("get", DATABASE)
				.chain(USERNAME).add("get").add("format", "?user=%s").end()
				.chain(PASSWORD).add("get").add("format", p.containsKey(USERNAME) ? "&password=%s" : "?password=%s").end()
				.filter("get", OPTIONS).toString();
	}
	
	private class UriBuilder extends FilteringStringBuilder {
		private Properties p;
		public UriBuilder(Properties p) {
			super();
			this.p = p;
			this.addFilter("get", FilteringStringBuilder.getMapEntry(p));
		}
	}
	
	private static Properties getConnectionPropertiesFromNode(Node node) {
		Properties p = new Properties();
		p.put(DRIVER, DriverLoader.getDriverStringForDialect(node.getDialect()));
		addIf(node.getDialect() == HiveDbDialect.MySql, HOST, node.getHost(), p);
		p.put(DATABASE, node.getDatabaseName());
	
		if(node.getPort() != 0)
			p.put(PORT, node.getPort());
		addIf(!empty(node.getUsername()), USERNAME, node.getUsername(), p);
		addIf(!empty(node.getPassword()),PASSWORD, node.getPassword(), p);
		addIf(!empty(node.getOptions()), OPTIONS, node.getOptions(), p);
		addIf(node.getDialect() == HiveDbDialect.MySql, OPTIONS, "&autoReconnect=true&autoReconnectForPools=true", p);
		return p;
	}
	
	@SuppressWarnings("unchecked")
	private static Map addIf(boolean b, Object key, Object value, Map map) {
		if(b)
			map.put(key, value);
		return map;
	}
}
