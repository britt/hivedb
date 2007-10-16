package org.hivedb.util.database;

import org.hivedb.UnsupportedDialectException;

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
		if(dialect.equals(H2))
			return HiveDbDialect.H2;
		else if(dialect.equals(DERBY))
			return HiveDbDialect.Derby;
		else if(dialect.equals(MYSQL))
			return HiveDbDialect.MySql;
		else
			throw new UnsupportedDialectException("Unkown database dialect.  HiveDB supports MySQL, H2 and Derby.");
	}
}
