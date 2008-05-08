package org.hivedb.hibernate;

import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Collection;

import org.hibernate.shards.ShardId;
import org.hibernate.shards.strategy.selection.ShardResolutionStrategyData;
import org.hibernate.shards.strategy.selection.ShardResolutionStrategyDataImpl;
import org.hivedb.Hive;
import org.hivedb.HiveLockableException;
import org.hivedb.configuration.EntityHiveConfig;
import org.hivedb.meta.Node;
import org.hivedb.util.database.HiveDbDialect;
import org.hivedb.util.database.test.Continent;
import org.hivedb.util.database.test.H2HiveTestCase;
import org.hivedb.util.database.test.WeatherReport;
import org.hivedb.util.database.test.WeatherReportImpl;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class HiveShardResolverTest extends H2HiveTestCase {
	
	@BeforeMethod
	@Override
	public void beforeMethod() {
		deleteDatabasesAfterEachTest = true;
		super.afterMethod();
		super.beforeMethod();
		try {
			getHive().addNode(new Node(Hive.NEW_OBJECT_ID, "node", getHiveDatabaseName(), "", HiveDbDialect.H2));
		} catch (HiveLockableException ex) {
			throw new RuntimeException(ex);
		}
	}
	
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
