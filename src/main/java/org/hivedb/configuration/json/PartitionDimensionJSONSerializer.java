package org.hivedb.configuration.json;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hivedb.PartitionDimension;
import org.hivedb.PartitionDimensionImpl;
import org.hivedb.util.database.JdbcTypeMapper;
import org.json.JSONException;
import org.json.JSONObject;

public class PartitionDimensionJSONSerializer implements JSONSerializer<PartitionDimension> {
  private final static Log log = LogFactory.getLog(PartitionDimensionJSONSerializer.class);
  private ResourceJSONSerializer resourceSerializer = new ResourceJSONSerializer();
  public static final String ID_KEY = "id";
  public static final String NAME_KEY = "name";
  public static final String TYPE_KEY = "type";
  public static final String URI_KEY = "uri";
  public static final String RESOURCES_KEY = "resources";

  public JSONObject toJSON(PartitionDimension entity) throws JSONException {
    JSONObject json = new JSONObject();
    json.put(ID_KEY, entity.getId());
    json.put(NAME_KEY, entity.getName());
    json.put(TYPE_KEY, JdbcTypeMapper.jdbcTypeToString(entity.getColumnType()));
    json.put(URI_KEY, entity.getIndexUri());
    json.put(RESOURCES_KEY, JSONTools.makeJSONArray(entity.getResources(), resourceSerializer));
    return json;
  }

  public PartitionDimension loadFromJSON(JSONObject json) throws JSONException {
    return new PartitionDimensionImpl(
      json.getInt(ID_KEY),
      json.getString(NAME_KEY),
      JdbcTypeMapper.parseJdbcType(json.getString(TYPE_KEY)),
      json.getString(URI_KEY),
      JSONTools.loadArray(json.getJSONArray(RESOURCES_KEY), resourceSerializer)
    );
  }
}

