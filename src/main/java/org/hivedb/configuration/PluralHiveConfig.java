package org.hivedb.configuration;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.hivedb.Hive;
import org.hivedb.HiveRuntimeException;
import org.hivedb.meta.Node;
import org.hivedb.util.Lists;
import org.hivedb.util.database.JdbcTypeMapper;
import org.hivedb.util.functional.DebugMap;
import org.hivedb.util.functional.Maps;

public class PluralHiveConfig implements EntityHiveConfig {
	private Map<String,EntityConfig> indexConfigurations = Maps.newHashMap();
	private Collection<Node> dataNodes = Lists.newArrayList();
	private Hive hive;
	
	public PluralHiveConfig(Map<String, EntityConfig> map, Hive hive) {
		this.indexConfigurations = map;
		this.dataNodes = hive.getNodes();
		this.hive = hive;
	}
	
	@SuppressWarnings("unchecked")
	public EntityConfig getEntityConfig(Class<?> clazz) {
		List<Class> ancestors = getAncestors(clazz);
		EntityConfig config = indexConfigurations.get(clazz.getName());
		if(config == null){
			for(Class ancestor : ancestors) {
				if(indexConfigurations.containsKey(ancestor.getName())){
					config = indexConfigurations.get(ancestor.getName());
					break;
				}
			}
		}
		return config;
	}

	public EntityConfig getEntityConfig(String className) {
		Class clazz = null;
		try {
			clazz = Class.forName(className);
		} catch (ClassNotFoundException e) {
			throw new HiveRuntimeException(e.getMessage(),e);
		}
		return getEntityConfig(clazz);
	}
	
	@SuppressWarnings("unused")
	private List<Class> getAncestors(Class<?> clazz) {
		List<Class> ancestors = Lists.newArrayList();
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

	public Collection<Node> getDataNodes() {
		return dataNodes;
	}

	public Hive getHive() {
		return hive;
	}

	public String getPartitionDimensionName() {
		return hive.getPartitionDimension().getName();
	}

	public Class<?> getPartitionDimensionType() {
		return JdbcTypeMapper.jdbcTypeToPrimitiveClass(hive.getPartitionDimension().getColumnType());
	}
}
