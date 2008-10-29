package org.hivedb;

import org.hivedb.configuration.persistence.HiveConfigurationSchema;
import org.hivedb.management.HiveConfigurationSchemaInstaller;
import org.hivedb.Node;
import org.hivedb.util.database.HiveDbDialect;
import org.hivedb.util.database.test.H2TestCase;
import org.hivedb.persistence.Schema;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

public class TestHiveLoading extends H2TestCase {
  Hive hive;

  @Before
  @Override
  public void beforeMethod() {
    deleteDatabasesAfterEachTest = true;
    super.afterMethod();
    super.beforeMethod();
    new HiveConfigurationSchemaInstaller(getConnectString(H2TestCase.TEST_DB)).run();
    hive = null;//Hive.load(getConnectString(H2TestCase.TEST_DB), CachingDataSourceProvider.getInstance());
  }

  public Collection<Schema> getSchemas() {
    return Arrays.asList(new Schema[]{
      new HiveConfigurationSchema(getConnectString(H2TestCase.TEST_DB))});
  }

  @Test
  public void testLoadingWithoutPartitionDimension() throws Exception {
    Hive hive = null;// = Hive.load(getConnectString(H2TestCase.TEST_DB), CachingDataSourceProvider.getInstance());
    assertNotNull(hive);
    Node node = getNode();
    hive.addNode(node);

    Hive fetched = null;// = Hive.load(getConnectString(H2TestCase.TEST_DB), CachingDataSourceProvider.getInstance());
    assertEquals(1, fetched.getNodes().size());
    assertEquals(node, fetched.getNode(node.getName()));
  }

  private Node getNode() {
    return new Node(Hive.NEW_OBJECT_ID, "nodal", H2TestCase.TEST_DB, "", HiveDbDialect.H2);
  }

  @Test
  public void testLoadWithPartitionDimension() throws Exception {
    Hive hive = null;// = Hive.create(getConnectString(H2TestCase.TEST_DB), "DIM", Types.INTEGER, CachingDataSourceProvider.getInstance(), null);
    assertNotNull(hive);
    Node node = getNode();
    hive.addNode(node);

    Hive fetched = null;//= Hive.load(getConnectString(H2TestCase.TEST_DB), CachingDataSourceProvider.getInstance());
    assertEquals(1, fetched.getNodes().size());
    assertEquals(node, fetched.getNode(node.getName()));
  }

  @Override
  public Collection<String> getDatabaseNames() {
    return Collections.singletonList(H2TestCase.TEST_DB);
  }


}
