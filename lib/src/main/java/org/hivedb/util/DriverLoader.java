package org.hivedb.util;

import java.util.HashMap;

import org.hivedb.HiveDbDialect;
import org.hivedb.UnsupportedDialectException;

/**
 * Provides default drivers for all HiveDbDialects.  
 * 
 * @author Justin McCarthy (jmccarthy@cafepress.com)
 */
public class DriverLoader {
	private static HashMap<HiveDbDialect, String> defaultDrivers = null;
	static {
		defaultDrivers = new HashMap<HiveDbDialect, String>();
		defaultDrivers.put(HiveDbDialect.MySql, "com.mysql.jdbc.Driver");
		defaultDrivers.put(HiveDbDialect.Derby,
				"org.apache.derby.jdbc.EmbeddedDriver");
	}

	public static void loadByDialect(HiveDbDialect dialect)
			throws ClassNotFoundException {
		Class.forName(defaultDrivers.get(dialect));
	}
	
	public static void load(String uri) throws ClassNotFoundException {
		loadByDialect(discernDialect(uri));
	}
	
	public static String getDriverClass(HiveDbDialect dialect) {
		return defaultDrivers.get(dialect);
	}
	
	/**
	 * From the connection URI determine the databhase type.
	 * @param uri
	 * @return 
	 */
	public static HiveDbDialect discernDialect(String uri)
	{
		if (uri.startsWith("jdbc:mysql:"))
			return HiveDbDialect.MySql;
		if (uri.startsWith("jdbc:derby:"))
			return HiveDbDialect.Derby;
		throw new UnsupportedDialectException("Could not discern the HiveDbDialect from the uri " + uri);
	}
	public static String parseConnectionUrl(String uri)
	{
		HiveDbDialect discernDialect = discernDialect(uri);
		if (discernDialect == HiveDbDialect.MySql)
			return "jdbc:mysql://${db.host}/${db.name}?user=${db.user}&amp;password=${db.password}";
		if (discernDialect == HiveDbDialect.Derby)
			return uri;
		throw new UnsupportedDialectException("Could not discern the HiveDbDialect from the uri " + uri);
	}
	public static String parseUsername(String uri)
	{
		HiveDbDialect discernDialect = discernDialect(uri);
		if (discernDialect == HiveDbDialect.MySql)
			return "jdbc:mysql://${db.host}/${db.name}?user=${db.user}&amp;password=${db.password}";
		if (discernDialect == HiveDbDialect.Derby)
			return uri;
		throw new UnsupportedDialectException("Could not discern the HiveDbDialect from the uri " + uri);
	}
	public static String parsePassword(String uri)
	{
		HiveDbDialect discernDialect = discernDialect(uri);
		if (discernDialect == HiveDbDialect.MySql)
			return "jdbc:mysql://${db.host}/${db.name}?user=${db.user}&amp;password=${db.password}";
		if (discernDialect == HiveDbDialect.Derby)
			return uri;
		throw new UnsupportedDialectException("Could not discern the HiveDbDialect from the uri " + uri);
	}
}
