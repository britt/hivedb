package org.hivedb.persistence;

import org.hivedb.HiveSemaphore;
import org.hivedb.HiveSemaphoreImpl;
import org.hivedb.Lockable;
import org.hivedb.configuration.persistence.HiveConfigurationSchema;
import org.hivedb.configuration.persistence.HiveSemaphoreDao;
import org.hivedb.util.Lists;
import org.hivedb.util.database.test.H2TestCase;
import org.junit.Assert;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

import java.util.Collection;

// TODO COmplete exception cases
public class TestHiveSemaphorePersistence extends H2TestCase {
  private static final String DB_NAME = "hive";

  @Test
  public void shouldCreateAndFetchHiveSemaphore() throws Exception {
    installHiveConfigurationSchema();
    HiveSemaphoreDao hsd = new HiveSemaphoreDao(getDataSource(DB_NAME));
    HiveSemaphore hs = hsd.create(new HiveSemaphoreImpl(Lockable.Status.writable, 1));
    assertEquals(hs, hsd.get());
  }

  @Test(expected = IllegalStateException.class)
  public void getShouldThrowIfTwoSemaphoresAreCreated() throws Exception {
    installHiveConfigurationSchema();
    HiveSemaphoreDao hsd = new HiveSemaphoreDao(getDataSource(DB_NAME));
    hsd.create(new HiveSemaphoreImpl(Lockable.Status.writable, 1));
    hsd.create(new HiveSemaphoreImpl(Lockable.Status.writable, 1));
    hsd.get();
  }

  @Test(expected = HiveSemaphoreDao.HiveSemaphoreNotFound.class)
  public void getShouldThrowIfNoSemaphoreExists() throws Exception {
    HiveSemaphoreDao hsd = new HiveSemaphoreDao(getDataSource(DB_NAME));
    hsd.get();
  }

  @Test
  public void testUpdate() throws Exception {
    installHiveConfigurationSchema();

    HiveSemaphoreDao hsd = new HiveSemaphoreDao(getDataSource(DB_NAME));
    HiveSemaphore hs = hsd.create(new HiveSemaphoreImpl(Lockable.Status.writable, 1));
    hs.incrementRevision();
    hsd.update(hs);

    HiveSemaphore hs2 = hsd.get();
    Assert.assertEquals(hs.getRevision(), hs2.getRevision());
    Assert.assertEquals(hs.getStatus(), hs2.getStatus());
  }

  private void installHiveConfigurationSchema() {
    new HiveConfigurationSchema(getConnectString(DB_NAME)).install();
  }


  @Override
  public Collection<String> getDatabaseNames() {
    return Lists.newList(DB_NAME);
  }
}
