package org.hivedb.util.database.test;

import java.io.Serializable;
import java.util.Collection;

import org.hivedb.Hive;
import org.hivedb.configuration.EntityConfig;
import org.hivedb.configuration.EntityHiveConfig;
import org.hivedb.hibernate.DataAccessObject;
import org.hivedb.hibernate.BaseDataAccessObjectFactory;
import org.hivedb.util.DataPersister;
import org.hivedb.util.scenarioBuilder.HiveScenarioTest;

public class TestHiveScenario {
	
	// Classes to generate and persist; this collection must be a subset of the entity classes in entityHiveConfg
	Collection<Class<?>> testEntityClasses;
	// Classes defined as entities. They may be in the list above or simply dependent entity classes.
	EntityHiveConfig entityHiveConfig;
	Collection<Class<?>> mappedClasses;
	Hive hive;
	public TestHiveScenario(Collection<Class<?>> testEntityClasses, EntityHiveConfig enitityHiveConfig, Collection<Class<?>> mappedClasses, Hive hive) {
		this.testEntityClasses = testEntityClasses;
		this.entityHiveConfig = enitityHiveConfig;
		this.mappedClasses = mappedClasses;
		this.hive = hive;
	}
	
	public void test() {
		for (Class<?> entityClass : testEntityClasses) {
			EntityConfig entityConfig = entityHiveConfig.getEntityConfig(entityClass);
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
				mappedClasses,
				clazz,
				hive).create();
	}
}