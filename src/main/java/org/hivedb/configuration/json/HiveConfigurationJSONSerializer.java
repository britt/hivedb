package org.hivedb.configuration.json;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hivedb.configuration.HiveConfiguration;
import org.hivedb.configuration.HiveConfigurationImpl;
import org.hivedb.meta.HiveSemaphore;
import org.hivedb.meta.Node;
import org.hivedb.meta.PartitionDimension;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.Collection;

public class HiveConfigurationJSONSerializer implements JSONSerializer<HiveConfiguration> {
  private final static Log log = LogFactory.getLog(HiveConfigurationJSONSerializer.class);
  private PartitionDimensionJSONSerializer partitionDimensionJSONSerializer = new PartitionDimensionJSONSerializer();
  private NodeJSONSerializer nodeJSONSerializer = new NodeJSONSerializer();
  private HiveSemaphoreJSONSerializer semaphoreJSONSerializer = new HiveSemaphoreJSONSerializer();
  public static final String DIMENSION_KEY = "dimension";
  public static final String SEMAPHORE_KEY = "semaphore";
  public static final String NODES_KEY = "nodes";

  public JSONObject toJSON(HiveConfiguration entity) throws JSONException {
    JSONObject json = new JSONObject();
    json.put(DIMENSION_KEY, partitionDimensionJSONSerializer.toJSON(entity.getPartitionDimension()));
    json.put(NODES_KEY, JSONTools.makeJSONArray(entity.getNodes(), nodeJSONSerializer));
    json.put(SEMAPHORE_KEY, semaphoreJSONSerializer.toJSON(entity.getSemaphore()));
    return json;
  }

  public HiveConfiguration loadFromJSON(JSONObject json) throws JSONException {
    PartitionDimension dimension = partitionDimensionJSONSerializer.loadFromJSON(json.getJSONObject(DIMENSION_KEY));
    Collection<Node> nodes = JSONTools.loadArray(json.getJSONArray(NODES_KEY), nodeJSONSerializer);
    HiveSemaphore semaphore = semaphoreJSONSerializer.loadFromJSON(json.getJSONObject(SEMAPHORE_KEY));
    return new HiveConfigurationImpl(nodes, dimension, semaphore);
  }
}

