package org.hivedb.meta;

import java.util.Arrays;
import java.util.Collection;

import org.hivedb.meta.persistence.IndexSchema;
import org.hivedb.util.TestObjectFactory;
import org.hivedb.util.database.test.H2TestCase;
import org.testng.annotations.Test;
public class TestIndexSchema extends H2TestCase {
	@Test
	public void testSchemaInstallation() {
		PartitionDimension dimension = TestObjectFactory.partitionDimension();
		dimension.setIndexUri(getConnectString("testDb"));
		IndexSchema schema = new IndexSchema( dimension );
		schema.install();
	}
	
	@Override
	public Collection<String> getDatabaseNames(){
		return Arrays.asList(new String[]{"testDb"});
	}
}
