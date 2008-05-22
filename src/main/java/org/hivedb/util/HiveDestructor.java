package org.hivedb.util;

import java.sql.DriverManager;
import java.sql.SQLException;

import org.hivedb.Hive;
import org.hivedb.Schema;
import org.hivedb.meta.Node;
import org.hivedb.util.database.DialectTools;
import org.hivedb.util.database.DriverLoader;
import org.hivedb.util.database.Schemas;

/**
 * Systematically destroys a given hive
 * 
 * @author mellwanger
 */
public class HiveDestructor {
	
	public void destroy(Hive hive) {
		for (Node node : hive.getNodes()) {
			destroy(node);
		}
		Schemas.uninstall(hive.getPartitionDimension());
		for (Node node : hive.getNodes()) {
			if (! node.getUri().equals(hive.getUri())) {
				shutdown(node);
			}
		}
		shutdown(hive);
	}
	
	private void destroy(Node node) {
		for (Schema schema : Schemas.getDataSchemas(node.getUri())) {
			Schemas.uninstall(schema, node.getUri());
		}
	}
	
	private void shutdown(Hive hive) {
		DriverLoader.initializeDriver(hive.getUri());
		try {
			DriverManager.getConnection(hive.getUri()).createStatement().execute(DialectTools.getDropDatabase(hive.getDialect(), hive.getName()));
		} catch (SQLException ex) {
			throw new RuntimeException(ex);
		}
	}
	
	private void shutdown(Node node) {
		DriverLoader.initializeDriver(node.getUri());
		try {
			DriverManager.getConnection(node.getUri()).createStatement().execute(DialectTools.getDropDatabase(node.getDialect(), node.getName()));
		} catch (SQLException ex) {
			throw new RuntimeException(ex);
		}
	}
}
