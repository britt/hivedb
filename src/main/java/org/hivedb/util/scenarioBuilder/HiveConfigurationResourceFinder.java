package org.hivedb.util.scenarioBuilder;

import java.util.Collection;

import org.hivedb.HiveException;
import org.hivedb.meta.Finder;
import org.hivedb.meta.Nameable;
import org.hivedb.util.functional.Filter;
import org.hivedb.util.functional.Predicate;

public class HiveConfigurationResourceFinder implements Finder {

	public <T extends Nameable> T findByName(Class<T> forClass, String name) throws HiveException {
		return (T) new Nameable() { 
			public String getName() {
				return Filter.grepSingle(new Predicate<ResourceIdentifiable>() {
					public boolean f(ResourceIdentifiable resourceIdentifiable) {
						return name.equals(resourceIdentifiable.getResourceName());
					}
				},
				hiveScenarioConfig.getPrimaryIndexIdentifiable().getResourceIdentifiables()).getResourceName();
			}
		};	
	}

	public <T extends Nameable> Collection<T> findCollection(Class<T> forClass) {
		// TODO Auto-generated method stub
		return null;
	}

}
