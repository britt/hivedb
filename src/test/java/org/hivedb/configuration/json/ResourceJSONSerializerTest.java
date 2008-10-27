package org.hivedb.configuration.json;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hivedb.meta.Resource;
import org.hivedb.meta.ResourceImpl;
import org.hivedb.meta.SecondaryIndex;
import org.json.JSONObject;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

import java.sql.Types;
import java.util.ArrayList;

public class ResourceJSONSerializerTest {
  private final static Log log = LogFactory.getLog(ResourceJSONSerializerTest.class);

  String jsonText = "{ \"id\" : 3 , \"name\" : \"aResource\" , \"type\" : \"integer\" , \"partition_resource\" : false , \"indexes\" : [] }";  

  @Test
  public void shouldSerializeToJSON() throws Exception {
    Resource resource = new ResourceImpl(3,"aResource", Types.INTEGER, false, new ArrayList<SecondaryIndex>());
    JSONObject json = new ResourceJSONSerializer().toJSON(resource);
    assertEquals(3, json.getInt("id"));
  }

  @Test
  public void shouldDeserializeFromJSON() throws Exception {
    Resource resource = new ResourceJSONSerializer().loadFromJSON(new JSONObject(jsonText));
    assertEquals(new Integer(3), resource.getId());
    assertEquals("aResource", resource.getName());
    assertEquals(Types.INTEGER, resource.getColumnType());
    assertEquals(false, resource.isPartitioningResource());
    assertEquals(0, resource.getSecondaryIndexes().size());
  }
}

