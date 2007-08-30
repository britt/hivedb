package org.hivedb.util;

import org.apache.commons.dbcp.BasicDataSource;
import org.hivedb.meta.HiveConfigurationSchema;
import org.hivedb.meta.persistence.HiveBasicDataSource;
import org.hivedb.meta.persistence.HiveSemaphoreDao;

public class InstallHiveGlobalSchema {
	/**
	 * This is going away
	 * @param connectString
	 * @return
	 */
	public static void install(String connectString) {
		try {
			new HiveConfigurationSchema(connectString).install();
			BasicDataSource ds = new HiveBasicDataSource(connectString);
			HiveSemaphoreDao dao = new HiveSemaphoreDao(ds);
			
			dao.create();
			
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
