package org.hivedb.management;

import javax.sql.DataSource;

import org.springframework.jdbc.support.incrementer.DataFieldMaxValueIncrementer;
import org.springframework.jdbc.support.incrementer.MySQLMaxValueIncrementer;

/**
 * Reasonable defaults hardcoded for key buffer size (100) and increment column name (JdbcKeyAuthority.COLUMN_NAME).
 * 
 * @author Justin McCarthy
 *
 * @param <T> Type of generated keys (Long or Integer)
 */
public class MySqlKeyAuthority<T extends Number> extends JdbcKeyAuthority {
	public MySqlKeyAuthority(DataSource ds, Class keySpace, Class returnType) {
		super(keySpace, returnType);
		this.setDataSource(ds);
		setIncrementer(this.getIncrementer(ds));
	}

	private DataFieldMaxValueIncrementer getIncrementer(DataSource ds) {
		MySQLMaxValueIncrementer incrementer = new MySQLMaxValueIncrementer();
		incrementer.setCacheSize(100);
		incrementer.setDataSource(ds);
		incrementer.setIncrementerName(getKeyspaceTableName());
		incrementer.setColumnName(COLUMN_NAME);
		return incrementer;
	}
}
