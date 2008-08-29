package org.hivedb.meta;

import org.hivedb.util.database.Schemas;
import org.hivedb.util.database.test.H2TestCase;
import org.junit.Test;

import java.sql.Types;
import java.util.Arrays;
import java.util.Collection;

public class TestIndexSchema extends H2TestCase {
	@Test
	public void testSchemaInstallation() {
		PartitionDimension dimension = new PartitionDimension("aDimension", Types.INTEGER);
		dimension.setIndexUri(getConnectString("testDb"));
		Schemas.install(dimension);
	}
	
	@Override
	public Collection<String> getDatabaseNames(){
		return Arrays.asList(new String[]{"testDb"});
	}
}
