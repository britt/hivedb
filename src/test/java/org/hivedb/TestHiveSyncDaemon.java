package org.hivedb;

import static org.testng.AssertJUnit.assertTrue;

import java.util.ArrayList;
import java.util.Observable;
import java.util.Observer;

import org.hivedb.meta.persistence.HiveSemaphoreDao;
import org.hivedb.util.database.test.HiveTest;
import org.hivedb.util.database.test.HiveTest.Config;
import org.testng.annotations.Test;

@Config(file="hive_default")
public class TestHiveSyncDaemon extends HiveTest {
	
	@Test
	public void testHiveSyncDaemon() {
		ArrayList<Observer> observers = new ArrayList<Observer>();
		observers.add(new DumbObserver());
		observers.add(new DumbObserver()); //why not two?
		HiveSyncDaemon daemon = new HiveSyncDaemon(getConnectString(getHiveDatabaseName()), observers);
		daemon.detectChanges();
		//the initial sync will register as changed
		for(Observer o : observers) {
			assertTrue(((DumbObserver) o).isChanged());
			((DumbObserver) o).setChanged(false);
		}
		
		HiveSemaphoreDao hsd = new HiveSemaphoreDao(getDataSource(getConnectString(getHiveDatabaseName())));
		hsd.incrementAndPersist();
		
		daemon.detectChanges();
		for(Observer o : observers) {
			assertTrue(((DumbObserver) o).isChanged());
		}
	}
	
	class DumbObserver implements Observer {
		private boolean changed = false;
		public boolean isChanged() {
			return changed;
		}
		public void setChanged(boolean changed) {
			this.changed = changed;
		}
		public void update(Observable o, Object arg) {
			this.changed = true;
		}
	}
}
