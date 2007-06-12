package org.hivedb.meta;

import org.hivedb.Hive;
import org.hivedb.meta.persistence.HiveBasicDataSource;
import org.hivedb.util.database.HiveTestCase;
import org.hivedb.util.functional.Atom;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DirectoryTest extends HiveTestCase {
	@BeforeMethod
	public void setUp() throws Exception {
		Hive hive = Hive.load(getConnectString(getHiveDatabaseName()));
		hive.addPartitionDimension(createPopulatedPartitionDimension());
		hive.addNode(Atom.getFirst(hive.getPartitionDimensions()), createNode(getHiveDatabaseName()));
		new IndexSchema(Atom.getFirst(hive.getPartitionDimensions())).install();
	}
	
//	@Test
	public void testExceptionBehavior() throws Exception {
		Hive hive = Hive.load(getConnectString(getHiveDatabaseName()));
		Directory dir = new Directory(Atom.getFirst(hive.getPartitionDimensions()), new HiveBasicDataSource(hive.getHiveUri()));
		dir.updatePrimaryIndexKey(createNode(getHiveDatabaseName()), new Integer(7));
	}
}
