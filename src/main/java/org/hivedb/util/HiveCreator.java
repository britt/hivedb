package org.hivedb.util;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.List;
import java.util.Map;

import org.hivedb.Hive;
import org.hivedb.meta.persistence.CachingDataSourceProvider;
import org.hivedb.meta.persistence.DataSourceProvider;
import org.hivedb.util.database.DialectTools;
import org.hivedb.util.database.HiveDbDialect;
import org.ho.yaml.Yaml;

/**
 * Systematically creates a hive from a given yaml configuration file
 * 
 * @author mellwanger
 */
public class HiveCreator {
	private DataSourceProvider dataSourceProvider = CachingDataSourceProvider.getInstance();
	private HiveDbDialect dialect;
	
	public HiveCreator(HiveDbDialect dialect) {
		try {
			Class.forName(DialectTools.getDriver(dialect));
		} catch (Exception ex) {
			throw new RuntimeException(String.format("Error initalizing the %s server.", dialect), ex);
		}
		this.dialect = dialect;
	}
	
	@SuppressWarnings("unchecked")
	public Hive load(String configFile) throws FileNotFoundException {
		Map<String, Map<String, ?>> configs = (Map<String, Map<String, ?>>) Yaml.load(new FileReader(configFile));
		if (configs == null || configs.size() != 1) {
			throw new RuntimeException(String.format("Zero or multipe hives defined in %s", configFile));
		}
		return load(configs.values().iterator().next());
	}
	
	@SuppressWarnings("unchecked")
	private Hive load(Map<String, ?> config) {
		Map<String, String> dimension = (Map<String, String>) config.get("dimension");
		List<Map<String, ?>> nodes = (List<Map<String, ?>>) config.get("nodes");
		List<Map<String, ?>> resources = (List<Map<String, ?>>) config.get("resources");
		return new BourneHive(dimension, dataSourceProvider, dialect).addNodes(nodes).addResources(resources).getHive();
	}
	
	public static void main(String[] args) throws FileNotFoundException {
		if (args.length != 4 || getDialect(args) == null || getConfigFile(args) == null) {
			usage();
		} else {
			new HiveCreator(getDialect(args)).load(getConfigFile(args));
		}
	}
	
	private static String getConfigFile(String args[]) {
		if (args[0].equalsIgnoreCase("-yml")) {
			return args[1];
		} else if (args[2].equalsIgnoreCase("-yml")) {
			return args[3];
		}
		return null;
	}
	
	private static HiveDbDialect getDialect(String dialect) {
		return HiveDbDialect.valueOf(dialect);
	}
	
	private static HiveDbDialect getDialect(String args[]) {
		if (args[0].equalsIgnoreCase("-dialect")) {
			return getDialect(args[1]);
		} else if (args[2].equalsIgnoreCase("-dialect")) {
			return getDialect(args[3]);
		}
		return null;
	}
	
	public static void usage() {
		System.out.println(new StringBuilder("USAGE: java -cp hivedb.jar org.hivedb.util.HiveCreator -dialect [h2|mysql] -yml <path_to_yaml_config>"));
	}
}
