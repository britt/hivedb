package org.hivedb.util.database;

public class SqlUtils {
	public static String camelToSql(String name) {
		return name.replaceAll("([A-Z])", "_$1").toLowerCase();
	}

	public static String singularize(String plural) {
		return plural.replaceAll("s$", "");
	}
}
