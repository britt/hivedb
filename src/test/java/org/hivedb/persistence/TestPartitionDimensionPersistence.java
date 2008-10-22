package org.hivedb.persistence;

import org.hivedb.Hive;
import org.hivedb.meta.PartitionDimensionImpl;
import org.hivedb.meta.Resource;
import org.hivedb.meta.persistence.PartitionDimensionDao;
import org.hivedb.util.database.test.HiveTest;
import org.hivedb.util.database.test.HiveTest.Config;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

import java.sql.Types;
import java.util.ArrayList;

@Config("hive_default")
public class TestPartitionDimensionPersistence extends HiveTest {

  @Test
  public void testCreate() throws Exception {
    PartitionDimensionDao dao = new PartitionDimensionDao(getDataSource(getConnectString(getHiveDatabaseName())));
    int initialSize = dao.loadAll().size();
    final PartitionDimensionImpl d = new PartitionDimensionImpl(Hive.NEW_OBJECT_ID, getHive().getPartitionDimension().getName(), Types.INTEGER, getConnectString(getHiveDatabaseName()), new ArrayList<Resource>());
    dao.create(d);
    assertTrue(d.getId() > 0);
    assertEquals(initialSize + 1, dao.loadAll().size());
  }
}
