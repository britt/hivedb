/**
 * HiveDB is an Open Source (LGPL) system for creating large, high-transaction-volume
 * data storage systems.
 */
package org.hivedb;

public enum HiveDbDialect {
	/**
	 * http://db.apache.org/ddlutils/databases/mysql.html
	 */
	MySql,
	/**
	 * http://db.apache.org/ddlutils/databases/derby.html
	 */
	Derby,
	/**
	 * http://www.h2database.com/
	 */
	H2
}