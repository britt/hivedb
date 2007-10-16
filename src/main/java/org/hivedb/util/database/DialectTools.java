package org.hivedb.util.database;

import org.hivedb.UnsupportedDialectException;

public class DialectTools {
	
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
			return "H2";
		else if(dialect == HiveDbDialect.Derby)
			return "Derby";
		else if(dialect == HiveDbDialect.MySql)
			return "MySQL";
		else
			throw new UnsupportedDialectException("Unkown database dialect.  HiveDB supports MySQL, H2 and Derby.");
	}
}
