package org.hivedb.configuration.json;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hivedb.Resource;
import org.hivedb.ResourceImpl;
import org.hivedb.util.database.JdbcTypeMapper;
import org.json.JSONException;
import org.json.JSONObject;

public class ResourceJSONSerializer implements JSONSerializer<Resource> {
  private final static Log log = LogFactory.getLog(ResourceJSONSerializer.class);
  public static final String ID_KEY = "id";
  public static final String NAME_KEY = "name";
  public static final String TYPE_KEY = "type";
  public static final String PARTITION_RESOURCE_KEY = "partition_resource";
  public static final String SECONDARY_INDEXES_KEY = "indexes";

  private SecondaryIndexJSONSerializer serializer = new SecondaryIndexJSONSerializer();

  public JSONObject toJSON(Resource entity) throws JSONException {
    JSONObject json = new JSONObject();
    json.put(ID_KEY, entity.getId());
    json.put(NAME_KEY, entity.getName());
    json.put(TYPE_KEY, JdbcTypeMapper.jdbcTypeToString(entity.getColumnType()));
    json.put(PARTITION_RESOURCE_KEY, entity.isPartitioningResource());   
    json.put(SECONDARY_INDEXES_KEY, JSONTools.makeJSONArray(entity.getSecondaryIndexes(), serializer));
    return json;
  }

  public Resource loadFromJSON(JSONObject json) throws JSONException {
    return new ResourceImpl(
      json.getInt(ID_KEY),
      json.getString(NAME_KEY),
      JdbcTypeMapper.parseJdbcType(json.getString(TYPE_KEY)),
      json.getBoolean(PARTITION_RESOURCE_KEY),
      JSONTools.loadArray(json.getJSONArray(SECONDARY_INDEXES_KEY), serializer)
    );
  }
}

