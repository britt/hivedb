package org.hivedb.configuration.json;

import org.json.JSONObject;
import org.json.JSONException;

public interface JSONSerializer<T> {
  JSONObject toJSON(T entity) throws JSONException;
  T loadFromJSON(JSONObject json) throws JSONException;
}
