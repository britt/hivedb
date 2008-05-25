package org.hivedb.persistence;

import static org.testng.AssertJUnit.assertEquals;

import java.sql.Types;
import java.util.ArrayList;

import org.hivedb.Hive;
import org.hivedb.meta.PartitionDimension;
import org.hivedb.meta.Resource;
import org.hivedb.meta.persistence.ResourceDao;
import org.hivedb.util.database.test.H2HiveTestCase;
import org.hivedb.util.database.test.HiveTest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TestResourcePersistence extends HiveTest {
	
	@Test
	public void testCreate() throws Exception {
		ResourceDao d = new ResourceDao(getDataSource(getConnectString(getHiveDatabaseName())));
		int intitialSize = d.loadAll().size();
		d.create(createResource());
		assertEquals(intitialSize+1,d.loadAll().size());
	}
}
