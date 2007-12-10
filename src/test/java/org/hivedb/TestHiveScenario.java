package org.hivedb;

import java.io.Serializable;

import org.hivedb.configuration.EntityConfig;
import org.hivedb.configuration.EntityHiveConfig;
import org.hivedb.hibernate.DataAccessObject;
import org.hivedb.hibernate.BaseDataAccessObjectFactory;
import org.hivedb.util.DataPersister;
import org.hivedb.util.scenarioBuilder.HiveScenarioTest;

public class TestHiveScenario {
	
	EntityHiveConfig entityHiveConfig;
	Hive hive;
	public TestHiveScenario(EntityHiveConfig enitityHiveConfig, Hive hive) {
		this.entityHiveConfig = enitityHiveConfig;
		this.hive = hive;
	}
	
	public void test() {
		for (EntityConfig entityConfig : entityHiveConfig.getEntityConfigs()) {
			int resourceInstanceCount = entityConfig.isPartitioningResource()
				? 2 // must equal primaryIndexKeyCount
				: 4; // make greater so that primaryIndexKeys are shared
			new HiveScenarioTest(
					entityHiveConfig, 
					hive, 
					entityConfig.getRepresentedInterface()).performTest(2,resourceInstanceCount, 
					new DataPersister(
							entityHiveConfig, 
							entityConfig.getRepresentedInterface(),
							getDao(entityConfig.getRepresentedInterface()),
							hive));
		}
	}
	private DataAccessObject<Object, Serializable> getDao(Class clazz) {	
		return new BaseDataAccessObjectFactory<Object,Serializable>(
				this.entityHiveConfig,
				clazz,
				hive).create();
	}
}