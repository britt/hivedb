package org.hivedb.meta;

import java.sql.Types;
import java.util.Arrays;
import java.util.Collection;

import org.hivedb.meta.persistence.IndexSchema;
import org.hivedb.util.database.test.H2TestCase;
import org.testng.annotations.Test;
public class TestIndexSchema extends H2TestCase {
	@Test
	public void testSchemaInstallation() {
		PartitionDimension dimension = new PartitionDimension("aDimension", Types.INTEGER);
		dimension.setIndexUri(getConnectString("testDb"));
		IndexSchema schema = new IndexSchema( dimension );
		schema.install();
	}
	
	@Override
	public Collection<String> getDatabaseNames(){
		return Arrays.asList(new String[]{"testDb"});
	}
}
