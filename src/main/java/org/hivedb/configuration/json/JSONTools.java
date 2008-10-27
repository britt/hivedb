package org.hivedb.configuration.json;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hivedb.util.Lists;
import org.json.JSONArray;
import org.json.JSONException;

import java.util.Collection;
import java.util.List;

public class JSONTools {
  private final static Log log = LogFactory.getLog(JSONTools.class);

  public static<T> Collection<T> loadArray(JSONArray array, JSONSerializer<T> serializer) throws JSONException {
    List<T> items = Lists.newArrayList();
    for(int i = 0; i < array.length(); i++) {
      items.add(serializer.loadFromJSON(array.getJSONObject(i)));
    }
    return items;
  }

  public static<T> JSONArray makeJSONArray(Collection<T> items, JSONSerializer<T> serializer) throws JSONException {
    JSONArray array = new JSONArray();
    for(T item : items) {
      array.put(serializer.toJSON(item));
    }
    return array;
  }
}

