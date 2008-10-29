package org.hivedb.directory;

import org.hivedb.HiveRuntimeException;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;

public class DefaultCacheKeyBuilderTest {
  private DefaultCacheKeyBuilder builder;

  @Before
  public void setUp() throws Exception {
    builder = new DefaultCacheKeyBuilder();
  }

  @Test
  public void shouldBuildCacheKeyFromPrimaryKey() throws Exception {
    Object key = new Integer(5);
    assertEquals("pri-5", builder.build(key));
  }

  @Test(expected = HiveRuntimeException.class)
  public void shouldThrowExceptionForNullPrimaryKey() throws Exception {
    builder.build(null);
  }

  @Test
  public void shouldBuildCacheKeyForResource() throws Exception {
    String resource = "aResource";
    Object resourceId = new Integer(5);
    assertEquals("res-aResource-5", builder.build(resource, resourceId));
  }

  @Test(expected = HiveRuntimeException.class)
  public void shouldThrowExceptionForNullResource() throws Exception {
    builder.build(null, 5);
  }

  @Test(expected = HiveRuntimeException.class)
  public void shouldThrowExceptionForNullResourceId() throws Exception {
    builder.build("aResource", null);
  }

  @Test
  public void shouldBuildCacheKeyForSecondaryKey() throws Exception {
    String resource = "aResource";
    Object resourceId = new Integer(5);
    String secondaryIndex = "aSecondary";
    Object secondaryIndexKey = new Integer(6);
    assertEquals("sec-aResource-5-aSecondary-6", builder.build(resource, secondaryIndex, secondaryIndexKey, resourceId));
  }

  @Test(expected = HiveRuntimeException.class)
  public void shouldThrowExceptionForNullResourceOnSecondaryKeyBuild() throws Exception {
    builder.build(null, "aSecondary", 5, 6);
  }

  @Test(expected = HiveRuntimeException.class)
  public void shouldThrowExceptionForNullResourceIdOnSecondaryKeyBuild() throws Exception {
    builder.build("aResource", "aSecondary", 5, null);
  }

  @Test(expected = HiveRuntimeException.class)
  public void shouldThrowExceptionForNullSecondaryKey() throws Exception {
    builder.build("aResource", null, 5, 6);
  }

  @Test(expected = HiveRuntimeException.class)
  public void shouldThrowExceptionForNullSecondaryKeyId() throws Exception {
    builder.build("aResource", "aSecondary", null, 6);
  }

  @Test
  public void shouldBuildPrimaryIndexCounterKeys() throws Exception {
    Object primaryIndexKey = new Integer(99);
    assertEquals(builder.build(primaryIndexKey) + "-res-counter", builder.buildCounterKey(primaryIndexKey, "res"));
  }

  @Test(expected = HiveRuntimeException.class)
  public void buildCounterKeyhouldThrowRuntimeExceptionForNullPrimaryKey() throws Exception {
    builder.buildCounterKey(null, "res");
  }

  @Test
  public void shouldBuildReferenceKeyBasedOnCounterAndResourceName() throws Exception {
    Integer primaryIndexKey = new Integer(77);
    String resource = "aResource";
    Long counter = new Long(42);
    assertEquals("ref-77-aResource-42", builder.buildReferenceKey(primaryIndexKey, resource, counter));
  }

  @Test(expected = HiveRuntimeException.class)
  public void buildReferenceKeyShouldThrowRuntimeExceptionForNullPrimaryKey() throws Exception {
    builder.buildReferenceKey(null, "aResource", new Long(42));
  }

  @Test(expected = HiveRuntimeException.class)
  public void buildReferenceKeyShouldThrowRuntimeExceptionForNullResource() throws Exception {
    builder.buildReferenceKey(new Integer(43), null, new Long(42));
  }

  @Test(expected = HiveRuntimeException.class)
  public void buildReferenceKeyShouldThrowRuntimeExceptionForNullResourceId() throws Exception {
    builder.buildReferenceKey(new Integer(42), "aResource", null);
  }
}
