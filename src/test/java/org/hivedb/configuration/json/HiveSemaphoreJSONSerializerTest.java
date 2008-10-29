package org.hivedb.configuration.json;

import org.hivedb.Lockable;
import org.hivedb.HiveSemaphore;
import org.hivedb.HiveSemaphoreImpl;
import org.json.JSONObject;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

public class HiveSemaphoreJSONSerializerTest {

  @Test
  public void shouldSerializeToJSON() throws Exception {
    HiveSemaphore semaphore = new HiveSemaphoreImpl(Lockable.Status.writable, 7);
    JSONObject json = new HiveSemaphoreJSONSerializer().toJSON(semaphore);
    assertEquals(0, json.getInt(HiveSemaphoreJSONSerializer.STATUS_KEY));
    assertEquals(7, json.getInt(HiveSemaphoreJSONSerializer.REVISION_KEY));
  }

  @Test
  public void shouldDeserializeFromJSON() throws Exception {
    String jsonText = "{ \"status\" : 0 , \"revision\" : 7 }";
    HiveSemaphore semaphore = new HiveSemaphoreJSONSerializer().loadFromJSON(new JSONObject(jsonText));
    assertEquals(Lockable.Status.writable, semaphore.getStatus());
    assertEquals(7, semaphore.getRevision());
  }
}
