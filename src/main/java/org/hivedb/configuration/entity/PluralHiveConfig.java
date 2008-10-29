package org.hivedb.configuration.entity;

import org.hivedb.HiveRuntimeException;
import org.hivedb.configuration.entity.EntityConfig;
import org.hivedb.configuration.entity.EntityHiveConfig;
import org.hivedb.util.Lists;
import org.hivedb.util.functional.DebugMap;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class PluralHiveConfig implements EntityHiveConfig {
	private Map<String,EntityConfig> indexConfigurations = new DebugMap<String, EntityConfig>();
	private String dimensionName;
	private Class<?> dimensionClass;
	
	public PluralHiveConfig(Map<String, EntityConfig> map, String dimensionName, Class<?> dimensionClass) {
		this.indexConfigurations = map;
		this.dimensionClass = dimensionClass;
		this.dimensionName = dimensionName;
	}
	
	@SuppressWarnings("unchecked")
	public EntityConfig getEntityConfig(Class<?> clazz) {
		EntityConfig config = indexConfigurations.get(clazz.getName());
		if(config == null){
			List<Class> ancestors = getAncestors(clazz);
			for(Class ancestor : ancestors) {
				if(indexConfigurations.containsKey(ancestor.getName())){
					config = indexConfigurations.get(ancestor.getName());
					break;
				}
			}
		}
		return config;
	}

	@SuppressWarnings("unchecked")
	public EntityConfig getEntityConfig(String className) {
		Class clazz = null;
		try {
			clazz = Class.forName(className);
		} catch (ClassNotFoundException e) {
			throw new HiveRuntimeException(e.getMessage(),e);
		}
		return getEntityConfig(clazz);
	}
	
	@SuppressWarnings("unchecked")
	private List<Class> getAncestors(Class<?> clazz) {
		List<Class> ancestors = Lists.newArrayList();
		if (!clazz.isInterface())
			ancestors.add(clazz.getSuperclass());
		ancestors.addAll(Arrays.asList(clazz.getInterfaces()));
		return ancestors;
	}
	
	public void add(String className, EntityConfig config) {
		this.indexConfigurations.put(className, config);
	}

	public Collection<EntityConfig> getEntityConfigs() {
		return indexConfigurations.values();
	}


	public String getPartitionDimensionName() {
		return this.dimensionName;
	}

	public Class<?> getPartitionDimensionType() {
		return this.dimensionClass;
	}
}
