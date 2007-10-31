package org.hivedb;

import org.hivedb.meta.AccessType;
import org.hivedb.meta.Node;
import org.hivedb.meta.PartitionDimension;
import org.hivedb.meta.directory.Directory;
import org.hivedb.meta.directory.DirectoryWrapper;
import org.hivedb.meta.directory.NodeResolver;
import org.hivedb.meta.persistence.HiveBasicDataSource;
import org.hivedb.util.AssertUtils;
import org.hivedb.util.database.test.H2HiveTestCase;
import org.hivedb.util.functional.Toss;
import org.hivedb.util.functional.Transform;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TestConnectionWriteLocking extends H2HiveTestCase {
	
	@BeforeMethod
	public void setUp() throws Exception {
		PartitionDimension dimension = createEmptyPartitionDimension();
		Hive hive = Hive.create(getConnectString(getHiveDatabaseName()), dimension.getName(), dimension.getColumnType());
		hive.addNode(createNode(getHiveDatabaseName()));
	}

	@Test
	public void testHiveLockingInMemory() throws Exception {
		final Hive hive = Hive.load(getConnectString(getHiveDatabaseName()));
		final Integer key = new Integer(13);

		hive.directory().insertPrimaryIndexKey(key);
		hive.updateHiveReadOnly(true);

		AssertUtils.assertThrows(new Toss(){

			public void f() throws Exception {
				hive.connection().getByPartitionKey(key, AccessType.ReadWrite);
			}}, HiveReadOnlyException.class);
	}

	@Test
	public void testHiveLockingPersistent() throws Exception {
		Hive hive = Hive.load(getConnectString(getHiveDatabaseName()));
		final Integer key = new Integer(13);

		hive.directory().insertPrimaryIndexKey(key);
		hive.updateHiveReadOnly(true);
		hive = null;

		final Hive fetchedHive = Hive.load(getConnectString(getHiveDatabaseName()));

		AssertUtils.assertThrows(new Toss(){

			public void f() throws Exception {
				fetchedHive.connection().getByPartitionKey(key, AccessType.ReadWrite);
			}}, HiveReadOnlyException.class);
	}

	@Test
	public void testNodeLockingInMemory() throws Exception {
		final Hive hive = Hive.load(getConnectString(getHiveDatabaseName()));
		final Integer key = new Integer(13);

		final PartitionDimension partitionDimension = hive.getPartitionDimension();
		hive.directory().insertPrimaryIndexKey(key);
		NodeResolver directory = new Directory(partitionDimension, new HiveBasicDataSource(hive.getUri()));
		for(Integer id : Transform.map(DirectoryWrapper.semaphoreToId(), directory.getKeySemamphoresOfPrimaryIndexKey(key)))
			getNode(partitionDimension,id).setReadOnly(true);

		AssertUtils.assertThrows(new Toss(){
			public void f() throws Exception {
				hive.connection().getByPartitionKey(key, AccessType.ReadWrite);
			}}, HiveReadOnlyException.class);
	}

	@Test
	public void testNodeLockingPersistent() throws Exception {
		Hive hive = Hive.load(getConnectString(getHiveDatabaseName()));
		final Integer key = new Integer(13);

		PartitionDimension partitionDimension = hive.getPartitionDimension();
		hive.directory().insertPrimaryIndexKey(key);
		NodeResolver directory = new Directory(partitionDimension, new HiveBasicDataSource(hive.getUri()));
		for(Integer id : Transform.map(DirectoryWrapper.semaphoreToId(), directory.getKeySemamphoresOfPrimaryIndexKey(key)))
			hive.updateNodeReadOnly(getNode(partitionDimension, id), true);
		hive = null;

		final Hive fetchedHive = Hive.load(getConnectString(getHiveDatabaseName()));

		AssertUtils.assertThrows(new Toss(){

			public void f() throws Exception {
				fetchedHive.connection().getByPartitionKey(key, AccessType.ReadWrite);
			}}, HiveReadOnlyException.class);

	}

	@Test
	public void testRecordLockingInMemory() throws Exception {
		final Hive hive = Hive.load(getConnectString(getHiveDatabaseName()));
		final Integer key = new Integer(13);

		hive.directory().insertPrimaryIndexKey(key);
		hive.directory().updatePrimaryIndexKeyReadOnly(key, true);

		AssertUtils.assertThrows(new Toss(){

			public void f() throws Exception {
				hive.connection().getByPartitionKey(key, AccessType.ReadWrite);
			}}, HiveReadOnlyException.class);
	}

	@Test
	public void testRecordLockingPersistent() throws Exception {
		Hive hive = Hive.load(getConnectString(getHiveDatabaseName()));
		final Integer key = new Integer(13);

		hive.directory().insertPrimaryIndexKey(key);
		hive.directory().updatePrimaryIndexKeyReadOnly(key, true);
		hive = null;

		final Hive fetchedHive = Hive.load(getConnectString(getHiveDatabaseName()));

		AssertUtils.assertThrows(new Toss(){

			public void f() throws Exception {
				fetchedHive.connection().getByPartitionKey(key, AccessType.ReadWrite);
			}}, HiveReadOnlyException.class);
	}

	private Node getNode(PartitionDimension dim, int id) throws HiveException {
		return dim.getNode(id);
	}
}
