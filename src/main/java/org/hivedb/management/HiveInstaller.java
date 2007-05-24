package org.hivedb.management;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Map;

import org.hivedb.HiveDbDialect;
import org.hivedb.HiveRuntimeException;
import org.hivedb.meta.GlobalSchema;
import org.hivedb.meta.command.HiveCommand;
import org.hivedb.meta.persistence.HiveBasicDataSource;
import org.hivedb.meta.persistence.HiveSemaphoreDao;
import org.hivedb.util.DriverLoader;
import org.hivedb.util.GetOpt;

public class HiveInstaller implements Runnable {
	private String uri;

	public HiveInstaller(String uri) {
		this.uri = uri;
	}

	public void run() {
		try {
			DriverLoader.loadByDialect(DriverLoader.discernDialect(this.uri));
			new GlobalSchema(uri).install();
			new HiveSemaphoreDao(new HiveBasicDataSource(uri)).create();
		} catch(Exception e) {
			throw new HiveRuntimeException(e.getMessage(), e);
		}
	}

	public static void main(String[] argz) {
		GetOpt opt = new GetOpt();
		opt.add("host", true);
		opt.add("db", true);
		opt.add("user", true);
		opt.add("pw", true);
		
		Map<String,String> argMap = opt.toMap(argz);
		if (!opt.validate())
			throw new IllegalArgumentException(
					"Usage: java -jar hivedb-jar-with-dependencies.jar -host <host> -db <database name> -user <username> -pw <password>");
		else {
			try{
				//Tickle driver
				Class.forName("com.mysql.jdbc.Driver");
				@SuppressWarnings("unused")
				Connection conn = DriverManager.getConnection(getConnectString(argMap));
			} catch(Exception e) {
				throw new HiveRuntimeException(e.getMessage());
			}
			System.out.println(getConnectString(argMap));
			new HiveInstaller(getConnectString(argMap)).run();
		}
	}

	private static String getConnectString(Map<String, String> argMap) {
		return String.format("jdbc:mysql://%s/%s?user=%s&password=%s", 
				argMap.get("host"), argMap.get("db"), argMap.get("user"), argMap.get("pw"));
	}
}
