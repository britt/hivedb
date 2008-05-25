package org.hivedb;

import java.util.Collection;

import org.hivedb.configuration.EntityConfig;
import org.hivedb.util.database.test.H2HiveTestCase;
import org.hivedb.util.database.test.HiveTest;
import org.hivedb.util.database.test.TestHiveScenario;
import org.hivedb.util.functional.Transform;
import org.hivedb.util.functional.Unary;
import org.testng.annotations.Test;

public class TestHiveScenarioWithH2 extends HiveTest {
//	@Test
	public void test() throws Exception {
		new TestHiveScenario(getEntityClasses(getEntityHiveConfig().getEntityConfigs()), getEntityHiveConfig(), getMappedClasses(), getHive()).test();
	}

	private Collection<Class<?>> getEntityClasses(Collection<EntityConfig> entityConfigs) {
		return Transform.map(new Unary<EntityConfig, Class<?>>() {
			public Class<?> f(EntityConfig entityConfig) {
				return entityConfig.getRepresentedInterface();
			}
		}, entityConfigs);	
	}
}
