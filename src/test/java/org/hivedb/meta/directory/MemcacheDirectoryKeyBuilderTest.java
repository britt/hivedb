package org.hivedb.meta.directory;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import org.hivedb.HiveRuntimeException;

public class MemcacheDirectoryKeyBuilderTest {
  private MemcacheDirectoryKeyBuilder builder;

  @Before
  public void setUp() throws Exception {
    builder = new MemcacheDirectoryKeyBuilder(new DefaultCacheKeyBuilder());
  }

  @Test
  public void shouldBuilderPrimaryIndexCounterKeys() throws Exception {
    Object primaryIndexKey = new Integer(99);
    assertEquals(builder.build(primaryIndexKey) + "-counter", builder.buildCounterKey(primaryIndexKey));
  }

  @Test(expected = HiveRuntimeException.class)
  public void buildCounterKeyhouldThrowRuntimeExceptionForNullPrimaryKey() throws Exception {
    builder.buildCounterKey(null);  
  }

  @Test
  public void shouldBuildReferenceKeyBasedOnCounterAndResourceName() throws Exception {
    Integer primaryIndexKey = new Integer(77);
    String resource = "aResource";
    Integer counter = new Integer(42);
    assertEquals("ref-77-aResource-42", builder.buildReferenceKey(primaryIndexKey, resource, counter));
  }

  @Test(expected = HiveRuntimeException.class)
  public void buildReferenceKeyShouldThrowRuntimeExceptionForNullPrimaryKey() throws Exception {
    builder.buildReferenceKey(null, "aResource", new Integer(42));  
  }

  @Test(expected = HiveRuntimeException.class)
  public void buildReferenceKeyShouldThrowRuntimeExceptionForNullResource() throws Exception {
    builder.buildReferenceKey(new Integer(43), null, new Integer(42));  
  }

  @Test(expected = HiveRuntimeException.class)
  public void buildReferenceKeyShouldThrowRuntimeExceptionForNullResourceId() throws Exception {
    builder.buildReferenceKey(new Integer(42), "aResource", null);  
  }
}
