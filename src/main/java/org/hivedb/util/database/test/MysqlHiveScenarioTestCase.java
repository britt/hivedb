package org.hivedb.util.database.test;

import java.util.Arrays;
import java.util.Collection;

import org.hivedb.configuration.SingularHiveConfig;
import org.hivedb.configuration.SingularHiveConfigImpl;
import org.hivedb.util.scenarioBuilder.HiveScenarioMarauderClasses;

public class MysqlHiveScenarioTestCase extends HiveMySqlTestCase {

	@Override
	public String getHiveDatabaseName() {
		return "storage_test";
	}

	private final String dimensionName = "pirate";
	protected Collection<? extends SingularHiveConfig> getHiveConfigs()
	{
		return Arrays.asList(new SingularHiveConfig[] {
				new SingularHiveConfigImpl(getOrCreateHive(dimensionName),
								  HiveScenarioMarauderClasses.getPirateConfiguration()),
				new SingularHiveConfigImpl(getOrCreateHive(dimensionName),
								   HiveScenarioMarauderClasses.getTreasureConfiguration())
		});
	}
}
