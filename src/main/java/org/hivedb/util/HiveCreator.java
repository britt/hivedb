package org.hivedb.util;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.List;
import java.util.Map;

import org.hivedb.Hive;
import org.hivedb.management.HiveInstaller;
import org.hivedb.meta.persistence.CachingDataSourceProvider;
import org.hivedb.meta.persistence.DataSourceProvider;
import org.hivedb.util.database.test.H2TestCase;
import org.ho.yaml.Yaml;

/**
 * Systematically creates a hive from a given yaml configuration file
 * 
 * @author mellwanger
 */
public class HiveCreator {
	private DataSourceProvider dataSourceProvider = CachingDataSourceProvider.getInstance();
	
	static {
		try {
			Class.forName("org.h2.Driver").newInstance();
		} catch (Exception e) {
			throw new RuntimeException("Error initalizing the h2 server.", e);
		}
	}
	
	public static void main(String[] argz) throws FileNotFoundException {
		HiveCreator creator = new HiveCreator();
		if(argz.length == 1) {
			creator.load(argz[0]);
		} else {
			usage();
		}
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
		return new BourneHive(dimension, dataSourceProvider).addNodes(nodes).addResources(resources).getHive();
	}
	
	public static void usage() {
		System.out.println(new StringBuilder("USAGE: java -cp hivedb.jar org.hivedb.util.HiveCreator CONFIG_FILE"));
	}
}
