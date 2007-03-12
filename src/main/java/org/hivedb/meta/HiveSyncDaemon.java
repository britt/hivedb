/**
 * HiveDB is an Open Source (LGPL) system for creating large, high-transaction-volume
 * data storage systems.
 */
package org.hivedb.meta;

import java.sql.SQLException;

import javax.sql.DataSource;

import org.hivedb.HiveException;
import org.hivedb.HiveRuntimeException;
import org.hivedb.meta.persistence.HiveBasicDataSource;
import org.hivedb.meta.persistence.HiveSemaphoreDao;
import org.hivedb.meta.persistence.PartitionDimensionDao;

public class HiveSyncDaemon extends Thread {

	// members
	long lastRun = 0;

	private Hive hive = null;

	public HiveSyncDaemon(Hive hive) {
		this.hive = hive;
		try {
			synchronize();
		} catch (HiveException ex) {
			throw new HiveRuntimeException(
					"HiveSyncDaemon unable to start due to sync error in underlying Hive: " + ex.getMessage(),ex);
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
			throw new HiveException(
					"Semaphore not found; make sure Hive is installed");
		}
	}

	public void run() {
		while (true) {
			try {
				synchronize();
				lastRun = System.currentTimeMillis();
				sleep(getConfiguredSleepPeriodMs());
			} catch (InterruptedException e) {
				// just don't care
			} catch (HiveException ex) {
				throw new HiveRuntimeException("Unable to synchronize due to problem with underlying Hive: " + ex.getMessage(),ex);
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
