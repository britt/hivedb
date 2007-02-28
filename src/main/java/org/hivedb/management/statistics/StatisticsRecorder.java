package org.hivedb.management.statistics;

import java.sql.SQLException;

import org.hivedb.meta.PartitionDimension;

// TODO Merge into VitalStatisticsDao.  This interface is not pulling its weight.

/**
 * A control object for recording statistics.  
 * @author bcrawford
 *
 */
public interface StatisticsRecorder {
	public void incrementChildRecordCount(PartitionDimension dimension, Object primaryindexKey, int count) throws SQLException;
	public void decrementChildRecordCount(PartitionDimension dimension, Object primaryindexKey, int count) throws SQLException;
}
