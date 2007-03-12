package org.hivedb.persistence;

import org.hivedb.HiveException;
import org.hivedb.meta.Hive;
import org.hivedb.meta.PartitionDimension;
import org.hivedb.util.DerbyTestCase;
import org.hivedb.util.HiveConfiguration;
import org.hivedb.util.scenarioBuilder.Undoable;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestSyncHive extends DerbyTestCase {
	//@Test
	public void insertPartitionDimensionAndSync() {
		final Hive hive = loadHive();
		final Hive myHive = new HiveConfiguration(hive);
		try {
			new Undoable() { public void f() throws Exception {
				final PartitionDimension partitionDimension = generatePartitionDimension();
				hive.addPartitionDimension(partitionDimension);
				
				assertEquality(myHive, hive);
				new Undo() { public void f() throws Exception {
					hive.deletePartitionDimension(partitionDimension);
				}};
			}

private Hive loadHive() throws HiveException {
			return Hive.load(getConnectString());
}}.cycle();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
	}
	
	private Hive loadHive() {
		// TODO Auto-generated method stub
		return null;
	}

	protected PartitionDimension generatePartitionDimension() {
		// TODO Auto-generated method stub
		return null;
	}

	@Test
	public void insertNodeAndSync() {
	}
	
	@Test
	public void insertResourceAndSync() {
	}
	
	@Test
	public void insertSecondaryIndexAndSync() {
		
	}
	
	public void assertEquality(Hive myHive, Hive theHive)
	{
		Assert.assertEquals(myHive, theHive);
	}
}
