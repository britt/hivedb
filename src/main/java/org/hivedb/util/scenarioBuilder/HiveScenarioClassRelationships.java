package org.hivedb.util.scenarioBuilder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import org.hivedb.util.InstallHiveIndexSchema;

public class HiveScenarioClassRelationships {
	
	private Map<Class, Collection<Class>> primaryToResourceMap = new DebugMap<Class,Collection<Class>>();
	private Map<Class, Class> resourceToPrimaryMap = new DebugMap<Class, Class>();
	private Map<String, Class> resourceNameToClassMap = new DebugMap<String, Class>();
	
	public HiveScenarioClassRelationships(HiveScenarioConfig hiveScenarioConfig)
	{
		for (Class<ResourceIdentifiable> resourceClass : hiveScenarioConfig.getResourceClasses()) {
			try {
				Class primaryClass = resourceClass.getConstructor().newInstance().getPrimaryIndexClass();
				if (!primaryToResourceMap.containsKey(primaryClass))
					primaryToResourceMap.put(primaryClass, new ArrayList<Class>());
				primaryToResourceMap.get(primaryClass).add(resourceClass);
				
				resourceToPrimaryMap.put(resourceClass, primaryClass);
				resourceNameToClassMap.put(InstallHiveIndexSchema.getResourceIdentifiablePrototype(primaryClass, resourceClass).getResourceName(), resourceClass);
			} catch (Exception e ) { throw new RuntimeException(e); }						
		}	
	}
	public Map<Class, Collection<Class>> getPrimaryToResourceMap() {
		return primaryToResourceMap;
	}
	public Map<String, Class> getResourceNameToClassMap() {
		return resourceNameToClassMap;
	}
	public Map<Class, Class> getResourceToPrimaryMap() {
		return resourceToPrimaryMap;
	}
}
