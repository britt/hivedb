package org.hivedb;

import static org.testng.AssertJUnit.assertTrue;

import java.util.ArrayList;
import java.util.Observable;
import java.util.Observer;

import org.hivedb.meta.persistence.HiveSemaphoreDao;
import org.hivedb.util.database.H2HiveTestCase;
import org.testng.annotations.Test;

public class TestHiveSyncDaemon extends H2HiveTestCase {
	
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
		
		HiveSemaphoreDao hsd = new HiveSemaphoreDao(getDataSource(getHiveDatabaseName()));
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
