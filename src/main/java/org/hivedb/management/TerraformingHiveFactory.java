package org.hivedb.management;

import java.util.List;

import org.hivedb.Hive;
import org.hivedb.hibernate.ConfigurationReader;

public class TerraformingHiveFactory {
	public static Hive colonize(String uri, List<Class<?>> persistedClasses) {
		new HiveInstaller(uri).run();
		ConfigurationReader reader = new ConfigurationReader(persistedClasses);
		reader.install(uri);
		return Hive.load(uri);
	}
}
