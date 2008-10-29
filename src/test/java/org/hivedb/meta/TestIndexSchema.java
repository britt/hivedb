package org.hivedb.meta;

import org.hivedb.util.database.Schemas;
import org.hivedb.util.database.test.H2TestCase;
import org.hivedb.PartitionDimensionImpl;
import org.junit.Test;

import java.sql.Types;
import java.util.Arrays;
import java.util.Collection;

public class TestIndexSchema extends H2TestCase {
  @Test
  public void testSchemaInstallation() {
    PartitionDimensionImpl dimension = new PartitionDimensionImpl("aDimension", Types.INTEGER);
    dimension.setIndexUri(getConnectString("testDb"));
    Schemas.install(dimension);
  }

  @Override
  public Collection<String> getDatabaseNames() {
    return Arrays.asList(new String[]{"testDb"});
  }
}
