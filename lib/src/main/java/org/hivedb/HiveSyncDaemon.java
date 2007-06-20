/**
 * HiveDB is an Open Source (LGPL) system for creating large, high-transaction-volume
 * data storage systems.
 */
package org.hivedb;

import java.util.Collection;
import java.util.Observable;
import java.util.Observer;

import javax.sql.DataSource;

import org.apache.log4j.Logger;
import org.hivedb.meta.persistence.HiveBasicDataSource;
import org.hivedb.meta.persistence.HiveSemaphoreDao;

public class HiveSyncDaemon extends Thread {
	Logger log = Logger.getLogger(HiveSyncDaemon.class);
	private Observable hiveStatus;
	private long lastRun = 0;
	private String hiveUri;
	private int lastRevision = Integer.MIN_VALUE;
	private int sleepPeriodMs = 5000;
	
	public HiveSyncDaemon(String uri, int sleepPeriodMs, Collection<Observer> observers) {
		this(uri, observers);
		this.sleepPeriodMs = sleepPeriodMs;
	}
	
	public HiveSyncDaemon(String uri, Collection<Observer> observers) {
		hiveUri = uri;
		hiveStatus = new HiveUpdateStatus();
		for(Observer o : observers)
			hiveStatus.addObserver(o);
	}

	private DataSource cachedDataSource = null;
	private DataSource getDataSource() {
		if (cachedDataSource == null) {
			cachedDataSource = new HiveBasicDataSource(hiveUri);
		}
		return cachedDataSource;
	}

	private int getLatestRevision() {
		return new HiveSemaphoreDao(getDataSource()).get().getRevision();
	}
	
	public synchronized void detectChanges() {
		int latestRevision = getLatestRevision();
		if (lastRevision != latestRevision)
			hiveStatus.notifyObservers();
		lastRevision = latestRevision;
	}

	public void run() {
		while (true) {
			try {
				detectChanges();
				lastRun = System.currentTimeMillis();
				sleep(getConfiguredSleepPeriodMs());
			} catch (Exception e) {
				log.warn("Error occurred while polling the HIveSemaphore", e);
			}
		}
	}

	/**
	 * Reports true if the sync thread has run within the last (2 *
	 * getConfiguredSleepPeriodMs()).
	 */
	public boolean isRunning() {
		return ((System.currentTimeMillis() - lastRun) < 2 * getConfiguredSleepPeriodMs());
	}

	public int getConfiguredSleepPeriodMs() {
		return sleepPeriodMs;
	}
	
	public void setSleepPeriodMs(int ms) {
		this.sleepPeriodMs = ms;
	}
	
	class HiveUpdateStatus extends Observable {
		public void notifyObservers() {
			super.setChanged();
			super.notifyObservers();
		}
	}
}
