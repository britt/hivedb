package org.hivedb.util;

import org.apache.commons.dbcp.BasicDataSource;
import org.hivedb.HiveDbDialect;
import org.hivedb.meta.GlobalSchema;
import org.hivedb.meta.Hive;
import org.hivedb.meta.persistence.HiveBasicDataSource;
import org.hivedb.meta.persistence.HiveSemaphoreDao;

public class InstallHiveGlobalSchema {
	/**
	 *  Convenience method to install a hive. If the hive already exists
	 *  this method will return it and do nothing.
	 * @param connectString
	 * @return
	 */
	public static Hive install(String connectString) {
		try {
			try {
				HiveDbDialect dialect =  DriverLoader.discernDialect(connectString);
				DriverLoader.loadByDialect(dialect);
				new GlobalSchema(connectString).install();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
			BasicDataSource ds = new HiveBasicDataSource(connectString);
			new HiveSemaphoreDao(ds).create();
			return Hive.load(connectString);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
