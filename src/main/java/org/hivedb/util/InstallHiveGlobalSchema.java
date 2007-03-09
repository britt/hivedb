package org.hivedb.util;

import org.apache.commons.dbcp.BasicDataSource;
import org.hivedb.HiveDbDialect;
import org.hivedb.meta.GlobalSchema;
import org.hivedb.meta.Hive;
import org.hivedb.meta.persistence.HiveBasicDataSource;
import org.hivedb.meta.persistence.HiveSemaphoreDao;

public class InstallHiveGlobalSchema {
	/**
	 * This is going away
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
			try {
				Hive.load(connectString);
			} catch (Exception e) {
				new HiveSemaphoreDao(ds).create();
			}
			return Hive.load(connectString);
			
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
