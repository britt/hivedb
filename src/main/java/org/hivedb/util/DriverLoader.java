package org.hivedb.util;

import java.util.HashMap;

import org.hivedb.HiveDbDialect;

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
		// TODO Integrate with configuration subsystem to provide for overrides
		Class.forName(defaultDrivers.get(dialect));
	}
}
