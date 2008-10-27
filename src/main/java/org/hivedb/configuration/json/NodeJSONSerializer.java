package org.hivedb.configuration.json;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hivedb.meta.Node;
import org.hivedb.util.database.DialectTools;
import org.json.JSONObject;
import org.json.JSONException;

public class NodeJSONSerializer implements JSONSerializer<Node> {
  private final static Log log = LogFactory.getLog(NodeJSONSerializer.class);
  public static final String ID_KEY = "id";
  public static final String NAME_KEY = "name";
  public static final String DATABASE_KEY = "database";
  public static final String HOST_KEY = "host";
  public static final String DIALECT_KEY = "dialect";
  public static final String USERNAME_KEY = "username";
  public static final String PASSWORD_KEY = "password";
  public static final String PORT_KEY = "port";
  public static final String OPTIONS_KEY = "options";
  public static final String CAPACITY_KEY = "capacity";

  public JSONObject toJSON(Node entity) throws JSONException {
    JSONObject json = new JSONObject();
    json.put(ID_KEY, entity.getId());
    json.put(NAME_KEY, entity.getName());
    json.put(HOST_KEY, entity.getHost());
    json.put(DATABASE_KEY, entity.getDatabaseName());
    json.put(USERNAME_KEY, entity.getUsername());
    json.put(PASSWORD_KEY, entity.getPassword());
    json.put(PORT_KEY, entity.getPort());
    json.put(OPTIONS_KEY, entity.getOptions());
    json.put(DIALECT_KEY, DialectTools.dialectToString(entity.getDialect()));
    json.put(CAPACITY_KEY, entity.getCapacity());
    return json;
  }

  public Node loadFromJSON(JSONObject json) throws JSONException {
    Node node;
    if(json.has(ID_KEY)) {
      node = new Node(
        json.getInt(ID_KEY),
        json.getString(NAME_KEY),
        json.getString(DATABASE_KEY),
        json.getString(HOST_KEY),
        DialectTools.stringToDialect(json.getString(DIALECT_KEY)));
    } else {
      node = new Node(
        json.getString(NAME_KEY),
        json.getString(DATABASE_KEY),
        json.getString(HOST_KEY),
        DialectTools.stringToDialect(json.getString(DIALECT_KEY)));
    }

    if(json.has(USERNAME_KEY))
      node.setUsername(json.getString(USERNAME_KEY));
    if(json.has(PASSWORD_KEY))
      node.setUsername(json.getString(PASSWORD_KEY));
    if(json.has(PASSWORD_KEY))
      node.setPassword(json.getString(PASSWORD_KEY));
    if(json.has(PORT_KEY))
      node.setPort(json.getInt(PORT_KEY));
    if(json.has(OPTIONS_KEY))
      node.setOptions(json.getString(OPTIONS_KEY));
    if(json.has(CAPACITY_KEY))
      node.setCapacity(json.getInt(CAPACITY_KEY));

    return node;
  }
}

