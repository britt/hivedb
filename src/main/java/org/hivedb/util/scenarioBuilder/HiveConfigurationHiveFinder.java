package org.hivedb.util.scenarioBuilder;

import java.util.Arrays;
import java.util.Collection;

import org.hivedb.meta.Finder;
import org.hivedb.meta.Nameable;
import org.hivedb.meta.PartitionDimension;

public class HiveConfigurationHiveFinder implements Finder {
	
	
	@SuppressWarnings("unused")
	private HiveScenarioConfig hiveScenarioConfig;
	private PartitionDimension partitionDimension;
	public HiveConfigurationHiveFinder(HiveScenarioConfig hiveScenarioConfig) {
		this.hiveScenarioConfig = hiveScenarioConfig;
		this.partitionDimension = PartitionDimensionCreator.create(hiveScenarioConfig);
	}
	@SuppressWarnings("unchecked")
	public <T extends Nameable> T findByName(Class<T> forClass, final String name) {
		
		return (T)partitionDimension;
	}
		
	@SuppressWarnings("unchecked")
	public <T extends Nameable> Collection<T> findCollection(Class<T> forClass) {
		return (Collection<T>) Arrays.asList(partitionDimension);
	}
}
