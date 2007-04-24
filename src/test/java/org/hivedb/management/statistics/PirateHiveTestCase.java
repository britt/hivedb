package org.hivedb.management.statistics;

import static org.testng.AssertJUnit.fail;

import java.util.Collection;

import org.apache.commons.dbcp.BasicDataSource;
import org.hivedb.meta.GlobalSchema;
import org.hivedb.meta.persistence.HiveBasicDataSource;
import org.hivedb.meta.persistence.HiveSemaphoreDao;
import org.hivedb.util.DerbyTestCase;
import org.hivedb.util.database.DerbyUtils;
import org.hivedb.util.functional.NumberIterator;
import org.hivedb.util.functional.Transform;
import org.hivedb.util.functional.Unary;
import org.hivedb.util.scenarioBuilder.HiveScenario;
import org.hivedb.util.scenarioBuilder.HiveScenarioMarauderConfig;


public abstract class PirateHiveTestCase extends DerbyTestCase {
	protected HiveScenario yeScenario = null;
	
	private Collection<String> dataNodes = null;
	public void setUp() {
		this.dataNodes = Transform.map(new Unary<Number, String>() {
			public String f(Number count) { 
				try {
					DerbyUtils.createDatabase("data"+count, userName, password);
					return "data"+count;
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}},
			new NumberIterator(3));
		
		
		try {
			// global schema
			new GlobalSchema(getConnectString()).install();
			BasicDataSource ds = new HiveBasicDataSource(getConnectString());
			new HiveSemaphoreDao(ds).create();
			yeScenario = HiveScenario.run(new HiveScenarioMarauderConfig(getConnectString(), dataNodes), 100, 10);
		} catch (Exception e) {
			e.printStackTrace();
			fail("Unable to initialize the hive: " + e.getMessage());
		} 
	}
}
