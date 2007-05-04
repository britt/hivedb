package org.hivedb.management;

import java.sql.Connection;
import java.sql.DriverManager;

import org.hivedb.HiveRuntimeException;
import org.hivedb.meta.GlobalSchema;
import org.hivedb.meta.persistence.HiveBasicDataSource;
import org.hivedb.meta.persistence.HiveSemaphoreDao;

public class HiveInstaller implements Runnable {
	private String uri;

	public HiveInstaller(String uri) {
		this.uri = uri;
	}

	public void run() {
		try {
			new GlobalSchema(uri).install();
			new HiveSemaphoreDao(new HiveBasicDataSource(uri)).create();
		} catch(Exception e) {
			throw new HiveRuntimeException(e.getMessage());
		}
	}

	public static void main(String[] argz) {
		if (argz.length != 1)
			throw new IllegalArgumentException(
					"The hive-installer accepts only one argument, a valid JDBC connection string. "
							+ argz.length);
		else {
			try{
				Class.forName("com.mysql.jdbc.Driver");
				@SuppressWarnings("unused")
				Connection conn = DriverManager.getConnection(argz[0].trim());
			} catch(Exception e) {
				throw new HiveRuntimeException(e.getMessage());
			}
			new HiveInstaller(argz[0].trim()).run();
		}
	}
}
