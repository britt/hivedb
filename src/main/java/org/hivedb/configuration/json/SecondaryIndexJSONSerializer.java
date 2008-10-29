package org.hivedb.configuration.json;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hivedb.SecondaryIndex;
import org.hivedb.util.database.JdbcTypeMapper;
import org.json.JSONObject;
import org.json.JSONException;

public class SecondaryIndexJSONSerializer implements JSONSerializer<SecondaryIndex> {
  private final static Log log = LogFactory.getLog(SecondaryIndexJSONSerializer.class);
  public static final String ID_KEY = "id";
  public static final String NAME_KEY = "name";
  public static final String TYPE_KEY = "type";

  public JSONObject toJSON(SecondaryIndex entity) throws JSONException {
    JSONObject json = new JSONObject();
    json.put(ID_KEY, entity.getId());
    json.put(NAME_KEY, entity.getName());
    json.put(TYPE_KEY, JdbcTypeMapper.jdbcTypeToString(entity.getColumnInfo().getColumnType()));
    return json;
  }

  public SecondaryIndex loadFromJSON(JSONObject json) throws JSONException {
    return new SecondaryIndex(json.getInt(ID_KEY), json.getString(NAME_KEY), JdbcTypeMapper.parseJdbcType(json.getString(TYPE_KEY)));
  }
}

