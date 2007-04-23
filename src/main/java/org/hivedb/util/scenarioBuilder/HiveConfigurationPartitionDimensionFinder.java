package org.hivedb.util.scenarioBuilder;

import java.util.Collection;

import org.hivedb.HiveException;
import org.hivedb.meta.Finder;
import org.hivedb.meta.Nameable;
import org.hivedb.util.ReflectionTools;
import org.hivedb.util.functional.Filter;
import org.hivedb.util.functional.Predicate;
import org.hivedb.util.functional.Unary;

public class HiveConfigurationPartitionDimensionFinder implements Finder {
	
	private HiveScenarioConfig hiveScenarioConfig;
	public HiveConfigurationPartitionDimensionFinder(HiveScenarioConfig hiveScenarioConfig) {
		this.hiveScenarioConfig = hiveScenarioConfig;
	}
	@SuppressWarnings("unchecked")
	public <T extends Nameable> T findByName(Class<T> forClass, final String name) throws HiveException {
		return (T) new Nameable() { 
			public String getName() {
				// Only one partition dimension is allowed in our configuration, so we can disregard the name argument here
				return hiveScenarioConfig.getPrimaryIndexIdentifiable().getPartitionDimensionName();
			}
		};	
	}
		
	public <T extends Nameable> Collection<T> findCollection(Class<T> forClass) {
		// TODO Auto-generated method stub
		return null;
	}
}
