package org.hivedb.hibernate;

import org.hibernate.shards.ShardId;
import org.hibernate.shards.strategy.selection.ShardResolutionStrategyData;
import org.hibernate.shards.strategy.selection.ShardResolutionStrategyDataImpl;
import org.hivedb.Hive;
import org.hivedb.configuration.EntityHiveConfig;
import org.hivedb.util.database.test.Continent;
import org.hivedb.util.database.test.HiveTest;
import org.hivedb.util.database.test.HiveTest.Config;
import org.hivedb.util.database.test.WeatherReport;
import org.hivedb.util.database.test.WeatherReportImpl;
import org.junit.Test;import static org.junit.Assert.assertNotNull;import static org.junit.Assert.assertTrue;

import java.util.Collection;

@Config(file="hive_default")
public class HiveShardResolverTest extends HiveTest {
	
	@Test
	public void testShardResolution() throws Exception{
		Hive hive = getHive();
		ConfigurationReader reader = new ConfigurationReader(Continent.class, WeatherReport.class);
		EntityHiveConfig config = reader.getHiveConfiguration();
		
		WeatherReport report = WeatherReportImpl.generate();
		Continent asia = new AsiaticContinent();
		
		HiveIndexer indexer = new HiveIndexer(hive);
		HiveShardResolver resolver = new HiveShardResolver(config, hive);
		
		indexer.insert(config.getEntityConfig(Continent.class), asia);
		indexer.insert(config.getEntityConfig(WeatherReport.class), report);

		ShardResolutionStrategyData continentData = new ShardResolutionStrategyDataImpl(Continent.class, asia.getName());
		ShardResolutionStrategyData reportData = new ShardResolutionStrategyDataImpl(WeatherReport.class, report.getReportId());
		
		Collection<ShardId> asiaIds = resolver.selectShardIdsFromShardResolutionStrategyData(continentData);
		Collection<ShardId> reportIds = resolver.selectShardIdsFromShardResolutionStrategyData(reportData);
		
		assertNotNull(asiaIds);
		assertNotNull(reportIds);
		assertTrue(asiaIds.size() > 0);
		assertTrue(reportIds.size() > 0);
		
		Collection<Integer> nodeIds = hive.directory().getNodeIdsOfPrimaryIndexKey(asia.getName());
		for(ShardId id : asiaIds)
			assertTrue(nodeIds.contains(id.getId()));
		
		nodeIds = hive.directory().getNodeIdsOfResourceId("WeatherReport", report.getReportId());
		for(ShardId id : reportIds)
			assertTrue(nodeIds.contains(id.getId()));
	}	
}
