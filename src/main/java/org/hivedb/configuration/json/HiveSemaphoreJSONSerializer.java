package org.hivedb.configuration.json;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hivedb.Lockable;
import org.hivedb.HiveSemaphore;
import org.hivedb.HiveSemaphoreImpl;
import org.json.JSONException;
import org.json.JSONObject;

public class HiveSemaphoreJSONSerializer implements JSONSerializer<HiveSemaphore> {
  private final static Log log = LogFactory.getLog(HiveSemaphoreJSONSerializer.class);
  public static final String REVISION_KEY = "revision";
  public static final String STATUS_KEY = "status";

  public JSONObject toJSON(HiveSemaphore entity) throws JSONException {
    JSONObject json = new JSONObject();
    json.put(REVISION_KEY, entity.getRevision());
    json.put(STATUS_KEY, entity.getStatus().getValue());
    return json;
  }

  public HiveSemaphore loadFromJSON(JSONObject json) throws JSONException {
    return new HiveSemaphoreImpl(Lockable.Status.getByValue((json.getInt(STATUS_KEY))), json.getInt(REVISION_KEY));
  }
}

