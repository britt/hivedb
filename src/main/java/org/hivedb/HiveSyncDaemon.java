/**
 * HiveDB is an Open Source (LGPL) system for creating large, high-transaction-volume
 * data storage systems.
 */
package org.hivedb;

import java.sql.SQLException;

import javax.sql.DataSource;

import org.apache.log4j.Logger;
import org.hivedb.meta.HiveSemaphore;
import org.hivedb.meta.PartitionDimension;
import org.hivedb.meta.persistence.HiveBasicDataSource;
import org.hivedb.meta.persistence.HiveSemaphoreDao;
import org.hivedb.meta.persistence.PartitionDimensionDao;

public class HiveSyncDaemon extends Thread {
	Logger log = Logger.getLogger(HiveSyncDaemon.class);

	long lastRun = 0;

	private Hive hive = null;

	public HiveSyncDaemon(Hive hive) {
		this.hive = hive;
		try {
			synchronize();
		} catch (Exception ex) {
			// suppress, as Hive may be installing.  Exception will be thrown 
			// when synchronize() is called after next waiting period.
			log.debug(this.getClass().getName() + " unable to sync Hive in constructor");
		}
	}

	private DataSource cachedDataSource = null;

	private DataSource getDataSource() {
		if (cachedDataSource == null) {
			cachedDataSource = new HiveBasicDataSource(hive.getHiveUri());
		}
		return cachedDataSource;
	}

	// TODO is this the right place for this unique operation?
	// A:  I think Hive.java might be a more appropriate place for this operation. --britt
	public void setReadOnly(boolean readOnly) throws SQLException {
		new HiveSemaphoreDao(getDataSource()).update(new HiveSemaphore(
				readOnly, hive.getRevision()));
	}

	public void forceSynchronize() throws HiveException {
		this.synchronize();
	}

	public void synchronize() throws HiveException {
		// update revision & locking, optionally triggering remaining sync
		// activies
		HiveSemaphoreDao hsd = new HiveSemaphoreDao(getDataSource());
		try {
			HiveSemaphore hs = null;
			hs = hsd.get();

			if (hive.getRevision() != hs.getRevision()) {
				PartitionDimensionDao pdd = new PartitionDimensionDao(
						getDataSource());
				// TODO: we should probably send Hive a "reload" here
				hive.getPartitionDimensions().clear();
				for (PartitionDimension p : pdd.loadAll())
					hive.getPartitionDimensions().add(p);
				hive.setRevision(hs.getRevision());
				hive.setReadOnly(hs.isReadOnly());
			}
		} catch (Exception e) {
			log.error("Semaphore not found; make sure Hive is installed");
			log.debug(e.getMessage());
			for (StackTraceElement element : e.getStackTrace())
				log.debug(element);
			throw new HiveException(
					"Semaphore not found; make sure Hive is installed", e);
		}
	}

	public void run() {
		while (true) {
			try {
				synchronize();
				lastRun = System.currentTimeMillis();
				sleep(getConfiguredSleepPeriodMs());
			} catch (Exception e) {
				// just don't care
			}
		}
	}

	public Hive getHive() {
		return this.hive;
	}

	/**
	 * Reports true if the sync thread has run within the last (2 *
	 * getConfiguredSleepPeriodMs()).
	 */
	public boolean isRunning() {
		return ((System.currentTimeMillis() - lastRun) < 2 * getConfiguredSleepPeriodMs());
	}

	public int getConfiguredSleepPeriodMs() {
		// TODO we haven't set any configuration standards yet,
		// and 5 seconds is a pretty good guess -- these systems
		// will be handling thousands of queries per second,
		// so we could probably shorten it even further without
		// presenting any significant load (as a percentage of
		// total query volume).
		return 5000;
	}
}
