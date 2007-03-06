package org.hivedb.meta;

import static org.testng.AssertJUnit.fail;

import java.sql.SQLException;

import org.hivedb.util.DerbyTestCase;
import org.hivedb.util.TestObjectFactory;
import org.testng.annotations.Test;
public class TestIndexSchema extends DerbyTestCase {
	@Test
	public void testSchemaInstallation() {
		PartitionDimension dimension = TestObjectFactory.partitionDimension();
		dimension.setIndexUri(getConnectString());
		IndexSchema schema = new IndexSchema( dimension );
		try {
			schema.install();
		} catch (SQLException e) {
			e.printStackTrace();
			fail("Exception thrown while installing schema: " + e.toString());
		}
	}
}
