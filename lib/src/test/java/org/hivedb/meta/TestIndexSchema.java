package org.hivedb.meta;

import static org.testng.AssertJUnit.fail;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;

import org.hivedb.util.TestObjectFactory;
import org.hivedb.util.database.DerbyTestCase;
import org.testng.annotations.Test;
public class TestIndexSchema extends DerbyTestCase {
	@Test
	public void testSchemaInstallation() {
		PartitionDimension dimension = TestObjectFactory.partitionDimension();
		dimension.setIndexUri(getConnectString("testDb"));
		IndexSchema schema = new IndexSchema( dimension );
		try {
			schema.install();
		} catch (SQLException e) {
			e.printStackTrace();
			fail("Exception thrown while installing schema: " + e.toString());
		}
	}
	
	@Override
	public Collection<String> getDatabaseNames(){
		return Arrays.asList(new String[]{"testDb"});
	}
}
