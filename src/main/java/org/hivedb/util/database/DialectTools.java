package org.hivedb.util.database;

import org.hibernate.dialect.H2Dialect;
import org.hibernate.dialect.MySQLInnoDBDialect;
import org.hivedb.persistence.UnsupportedDialectException;

public class DialectTools {
	
	public static final String MYSQL = "MySQL";
	public static final String DERBY = "Derby";
	public static final String H2 = "H2";

	/**
	 * Get the SQL fragment to create an auto-incrementing key sequence for a database dialect.
	 * @param dialect
	 * @return
	 */
	public static String getNumericPrimaryKeySequenceModifier(HiveDbDialect dialect) {
		String statement = "";
		if (dialect == HiveDbDialect.MySql)
			statement = "int not null auto_increment primary key";
		else if (dialect == HiveDbDialect.H2)
			statement = "int not null auto_increment primary key";
		else if (dialect == HiveDbDialect.Derby)
			statement = "int not null generated always as identity primary key";
		else 
			throw new UnsupportedDialectException("Unsupported dialect: " + dialect.toString());
		return statement;
	}
	
	/**
	 * Boolean types vary from database to database, this method returns the smallest
	 * available type to be used as a boolean.
	 * @param dialect A HiveDbDialect enum
	 * @return
	 */
	public static String getBooleanTypeForDialect(HiveDbDialect dialect)
	{
		if (dialect.equals(HiveDbDialect.MySql) || dialect.equals(HiveDbDialect.H2))
			return "BOOLEAN";
		else if (dialect.equals(HiveDbDialect.Derby))
			return "INT";			
		throw new UnsupportedDialectException("No option boolean option configured for " + dialect.name());
	}
	
	
	public static String dialectToString(HiveDbDialect dialect) {
		if(dialect == HiveDbDialect.H2)
			return H2;
		else if(dialect == HiveDbDialect.Derby)
			return DERBY;
		else if(dialect == HiveDbDialect.MySql)
			return MYSQL;
		else
			throw new UnsupportedDialectException("Unkown database dialect.  HiveDB supports MySQL, H2 and Derby.");
	}
	
	public static HiveDbDialect stringToDialect(String dialect) {
		if(dialect.toLowerCase().equals(H2.toLowerCase()))
			return HiveDbDialect.H2;
		else if(dialect.toLowerCase().equals(DERBY.toLowerCase()))
			return HiveDbDialect.Derby;
		else if(dialect.toLowerCase().equals(MYSQL.toLowerCase()))
			return HiveDbDialect.MySql;
		else
			throw new UnsupportedDialectException("Unkown database dialect.  HiveDB supports MySQL, H2 and Derby.");
	}
	
	public static String getDropDatabase(HiveDbDialect dialect, String name) {
		switch (dialect) {
			case H2: return "SHUTDOWN";
			case MySql: return String.format("drop database %s", name);
			default: return String.format("drop database %s", name);
		}
	}
	
	public static String getDriver(HiveDbDialect dialect) {
		switch (dialect) {
			case H2: return "org.h2.Driver";
			case MySql: return "com.mysql.jdbc.Driver";
			default: throw new RuntimeException("Unsupported Dialect: " + dialect);
		}
	}
	
	public static String getHiveTestUri(HiveDbDialect dialect) {
		switch (dialect) {
			case H2: return "jdbc:h2:mem:testDb;LOCK_MODE=3";
			case MySql: return "jdbc:mysql://localhost/?user=test&password=test";
			default: throw new RuntimeException("Unsupported Dialect: " + dialect);
		}
	}

  public static Class getHibernateDialect(HiveDbDialect dialect) {
    return dialect == HiveDbDialect.H2 ? H2Dialect.class : MySQLInnoDBDialect.class;
  }

  public static HiveDbDialect getHiveDbDialectForHibernateDialect(Class hibernateDialect) {
    if(hibernateDialect.isAssignableFrom(H2Dialect.class))
      return HiveDbDialect.H2;
    else if(hibernateDialect.isAssignableFrom(MySQLInnoDBDialect.class))
      return HiveDbDialect.MySql;
    else
      throw new UnsupportedDialectException("Unkown database dialect.  HiveDB supports MySQL and H2.");
  }
}
