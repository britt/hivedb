/**
 * HiveDB is an Open Source (LGPL) system for creating large, high-transaction-volume
 * data storage systems.
 */
package org.hivedb.meta;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;

import org.hivedb.Lockable;
import org.hivedb.Schema;
import org.hivedb.Lockable.Status;
import org.hivedb.util.HiveUtils;
import org.hivedb.util.database.HiveDbDialect;
import org.hivedb.util.database.JdbcUriFormatter;
import org.hivedb.util.database.Schemas;

/**
 * Node models a database instance suitable for storage of partitioned Data.
 * 
 * @author Kevin Kelm (kkelm@fortress-consulting.com)
 * @author Andy Likuski (alikuski@cafepress.com)
 */
public class Node implements Comparable<Node>, Cloneable, IdAndNameIdentifiable<Integer>, Lockable {
	private int id,port;
	private String name, host,databaseName, username, password, options;
	private Status status = Status.writable;
	private double capacity;
	private HiveDbDialect dialect;

	public Node(int id, String name, String databaseName, String host, HiveDbDialect dialect) {
		this(name, databaseName, host, dialect);
		this.id = id;
		
	}
	
	public Node(String name, String databaseName, String host, HiveDbDialect dialect){
		this.name = name;
		this.databaseName = databaseName;
		this.host = host;
		this.dialect = dialect;
	}
	
	public Node() {}
	
	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public String getDatabaseName() {
		return databaseName;
	}

	public void setDatabaseName(String databaseName) {
		this.databaseName = databaseName;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getOptions() {
		return options;
	}

	public void setOptions(String options) {
		this.options = options;
	}

	public HiveDbDialect getDialect() {
		return dialect;
	}

	public void setDialect(HiveDbDialect dialect) {
		this.dialect = dialect;
	}

	public void setId(int id) {
		this.id = id;
	}
	
	public Integer getId() {
		return id;
	}
	
	public Status getStatus() {
		return status;
	}
	public void setStatus(Status status) {
		this.status = status;
	}
	
	public String getUri() {
		return new JdbcUriFormatter(this).getUri();
	}
	
	public double getCapacity() {
		return capacity;
	}
	public void setCapacity(double capacity) {
		this.capacity = capacity;
	}
	
	public void updateId(int id) {
		this.id = id;
	}	

	public String getName() {
		return name;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	public void setName(String name) {
		this.name = name;
	}

	public boolean equals(Object obj)
	{
		return obj.hashCode() == hashCode();
	}
	public int hashCode() {
		return HiveUtils.makeHashCode(new Object[] {
				id,port,name, host,databaseName, username, password, options,status,capacity,dialect
		});
	}
	public String toString()
	{
		return HiveUtils.toDeepFormatedString(this, 
										"Id", 		getId(), 
										"Name", 	getName(), 
										"Uri", 		getUri(), 
										"Status",	status
										);									
	}

	public int compareTo(Node o) {
		return getUri().compareTo(o.getUri());
	}
}
