package org.hivedb.configuration.yaml;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hivedb.HiveRuntimeException;
import org.hivedb.Lockable;
import org.hivedb.configuration.HiveConfiguration;
import org.hivedb.configuration.HiveConfigurationImpl;
import org.hivedb.meta.*;
import org.hivedb.util.Lists;
import org.hivedb.util.database.HiveDbDialect;
import org.hivedb.util.database.JdbcTypeMapper;
import org.hivedb.util.functional.Factory;
import org.ho.yaml.Yaml;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

public class YamlHiveConfigurationLoader implements Factory<HiveConfiguration> {
  private final static Log log = LogFactory.getLog(YamlHiveConfigurationLoader.class);
  private String configFile;
  private static final String HIVE_KEY = "hive";
  private HiveConfiguration instance = null;
  private long lastLoaded;
  private static final String SEMAPHORE_KEY = "semaphore";
  private static final String SEMAPHORE_VERSION_KEY = "version";
  private static final String SEMAPHORE_STATUS_KEY = "status";

  public YamlHiveConfigurationLoader(String configFile) {
    this.configFile = configFile;
  }

  public HiveConfiguration newInstance() {
    if (instance == null || getConfigModificationTime() > lastLoaded)
      instance = loadConfig();
    return instance;
  }

  private long getConfigModificationTime() {
    return new File(getConfigFile()).lastModified();
  }

  private HiveConfiguration loadConfig() {
    if (new File(getConfigFile()).exists()) {
      Map<String, Map<String, ?>> configMap = (Map<String, Map<String, ?>>) Yaml.load(getConfigFile());
      Map<String, ?> hiveMap = configMap.get(HIVE_KEY);
      PartitionDimension dimension = buildPartitionDimension(hiveMap);
      Collection<Node> nodes = buildNodes(hiveMap);
      HiveSemaphore semaphore = buildHiveSemaphore(hiveMap);
      return new HiveConfigurationImpl(getFileUri(), nodes, dimension, semaphore);
    } else {
      throw new HiveRuntimeException("Could not find file " + getConfigFile());
    }
  }

  public String getFileUri() {
    return String.format("file://%s", new File(getConfigFile()).getAbsolutePath());
  }

  private HiveSemaphore buildHiveSemaphore(Map<String, ?> hiveMap) {
    if (hiveMap.containsKey(SEMAPHORE_KEY)) {
      Map<String, ?> semaphoreMap = (Map<String, ?>) hiveMap.get(SEMAPHORE_KEY);
      Integer version = (Integer) semaphoreMap.get(SEMAPHORE_VERSION_KEY);
      Lockable.Status status = Lockable.Status.valueOf(semaphoreMap.get(SEMAPHORE_STATUS_KEY).toString());
      return new HiveSemaphoreImpl(status, version);
    } else {
      return new HiveSemaphoreImpl(Lockable.Status.writable, 1);
    }
  }

  private Collection<Node> buildNodes(Map<String, ?> hiveMap) {
    if (hiveMap.containsKey("nodes")) {
      Collection<Map<String, ?>> nodeConfigs = (Collection<Map<String, ?>>) hiveMap.get("nodes");
      Collection<Node> nodes = Lists.newArrayList();
      int id = 0;
      for (Map<String, ?> nodeConfig : nodeConfigs) {
        nodes.add(buildNode(nodeConfig, id++));
      }
      return nodes;
    } else {
      return new ArrayList<Node>();
    }
  }

  private Node buildNode(Map<String, ?> nodeConfig, int id) {
    return new Node(
      id,
      nodeConfig.get("name").toString(),
      nodeConfig.get("dbName").toString(),
      nodeConfig.get("host").toString(),
      HiveDbDialect.valueOf(nodeConfig.get("dialect").toString()));
  }

  private PartitionDimension buildPartitionDimension(Map<String, ?> hiveMap) {
    Map<String, ?> dimensionConfig = (Map<String, ?>) hiveMap.get("dimension");
    Collection<Map<String, ?>> resourceConfigs = (Collection<Map<String, ?>>) hiveMap.get("resources");
    Collection<Resource> resources = Lists.newArrayList();
    int id = 0;
    for (Map<String, ?> resourceConfig : resourceConfigs) {
      resources.add(buildResource(resourceConfig, id));
    }
    return new PartitionDimensionImpl(dimensionConfig.get("name").toString(), JdbcTypeMapper.parseJdbcType(dimensionConfig.get("type").toString()), resources);
  }

  private Resource buildResource(Map<String, ?> resourceConfig, int id) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  public String getConfigFile() {
    return configFile;
  }

  public void setConfigFile(String configFile) {
    this.configFile = configFile;
  }
}

