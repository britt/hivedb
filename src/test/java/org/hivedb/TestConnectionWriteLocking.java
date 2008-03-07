package org.hivedb;

import org.hivedb.Lockable.Status;
import org.hivedb.meta.AccessType;
import org.hivedb.meta.Node;
import org.hivedb.meta.PartitionDimension;
import org.hivedb.meta.directory.Directory;
import org.hivedb.meta.directory.DirectoryWrapper;
import org.hivedb.meta.directory.NodeResolver;
import org.hivedb.meta.persistence.HiveBasicDataSource;
import org.hivedb.util.AssertUtils;
import org.hivedb.util.database.HiveDbDialect;
import org.hivedb.util.database.test.H2HiveTestCase;
import org.hivedb.util.functional.Toss;
import org.hivedb.util.functional.Transform;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TestConnectionWriteLocking extends H2HiveTestCase {
	
	@BeforeMethod
	public void setUp() throws Exception {
		getHive().addNode(new Node(Hive.NEW_OBJECT_ID, "node", getHiveDatabaseName(), "", HiveDbDialect.H2));
	}

	@Test
	public void testHiveLockingInMemory() throws Exception {
		final Hive hive = getHive();
		final String key = new String("North America");

		hive.directory().insertPrimaryIndexKey(key);
		hive.updateHiveStatus(Status.readOnly);

		AssertUtils.assertThrows(new Toss(){

			public void f() throws Exception {
				hive.connection().getByPartitionKey(key, AccessType.ReadWrite);
			}}, HiveLockableException.class);
	}

	@Test
	public void testHiveLockingPersistent() throws Exception {
		Hive hive = Hive.load(getConnectString(getHiveDatabaseName()));
		final String key = new String("Stoatia");

		hive.directory().insertPrimaryIndexKey(key);
		hive.updateHiveStatus(Status.readOnly);
		hive = null;

		final Hive fetchedHive = Hive.load(getConnectString(getHiveDatabaseName()));

		AssertUtils.assertThrows(new Toss(){

			public void f() throws Exception {
				fetchedHive.connection().getByPartitionKey(key, AccessType.ReadWrite);
			}}, HiveLockableException.class);
	}

	@Test
	public void testNodeLockingInMemory() throws Exception {
		final Hive hive = Hive.load(getConnectString(getHiveDatabaseName()));
		final String key = new String("Antarctica");

		final PartitionDimension partitionDimension = hive.getPartitionDimension();
		hive.directory().insertPrimaryIndexKey(key);
		NodeResolver directory = new Directory(partitionDimension, new HiveBasicDataSource(hive.getUri()));
		for(Integer id : Transform.map(DirectoryWrapper.semaphoreToId(), directory.getKeySemamphoresOfPrimaryIndexKey(key)))
			hive.getNode(id).setStatus(Status.readOnly);

		AssertUtils.assertThrows(new Toss(){
			public void f() throws Exception {
				hive.connection().getByPartitionKey(key, AccessType.ReadWrite);
			}}, HiveLockableException.class);
	}

	@Test
	public void testNodeLockingPersistent() throws Exception {
		Hive hive = Hive.load(getConnectString(getHiveDatabaseName()));
		final String key = new String("Asia");

		PartitionDimension partitionDimension = hive.getPartitionDimension();
		hive.directory().insertPrimaryIndexKey(key);
		NodeResolver directory = new Directory(partitionDimension, new HiveBasicDataSource(hive.getUri()));
		for(Integer id : Transform.map(DirectoryWrapper.semaphoreToId(), directory.getKeySemamphoresOfPrimaryIndexKey(key)))
			hive.updateNodeStatus(hive.getNode(id), Status.readOnly);
		hive = null;

		final Hive fetchedHive = Hive.load(getConnectString(getHiveDatabaseName()));

		AssertUtils.assertThrows(new Toss(){

			public void f() throws Exception {
				fetchedHive.connection().getByPartitionKey(key, AccessType.ReadWrite);
			}}, HiveLockableException.class);

	}

	@Test
	public void testRecordLockingInMemory() throws Exception {
		final Hive hive = Hive.load(getConnectString(getHiveDatabaseName()));
		final String key = new String("Atlantis");

		hive.directory().insertPrimaryIndexKey(key);
		hive.directory().updatePrimaryIndexKeyReadOnly(key, true);

		AssertUtils.assertThrows(new Toss(){

			public void f() throws Exception {
				hive.connection().getByPartitionKey(key, AccessType.ReadWrite);
			}}, HiveLockableException.class);
	}

	@Test
	public void testRecordLockingPersistent() throws Exception {
		Hive hive = Hive.load(getConnectString(getHiveDatabaseName()));
		final String key = new String("Africa");

		hive.directory().insertPrimaryIndexKey(key);
		hive.directory().updatePrimaryIndexKeyReadOnly(key, true);
		hive = null;

		final Hive fetchedHive = Hive.load(getConnectString(getHiveDatabaseName()));

		AssertUtils.assertThrows(new Toss(){

			public void f() throws Exception {
				fetchedHive.connection().getByPartitionKey(key, AccessType.ReadWrite);
			}}, HiveLockableException.class);
	}
}
