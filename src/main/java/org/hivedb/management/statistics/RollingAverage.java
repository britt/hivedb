package org.hivedb.management.statistics;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A class for measuring rolling averages of numerical data over a specified
 * time window.
 * 
 * <p>
 * The time window of interest is subdivided into intervals (buckets) into which
 * data is collected. The number of intervals determines the accuracy at which
 * data is measured -- all data added within a given interval "falls out" of the
 * window as a group.
 * 
 * <p>
 * As an extreme example, consider a rolling average that was being calculated
 * over a 60 second window with twelve 5 second intervals. At t = 59, the
 * average would reflect data from t = 1 to 59. However, at t = 62, the average
 * would reflect data from t = 6 to 62 as data from the first 5 second interval
 * would have been discarded.
 * 
 * <p>
 * Increasing the number of intervals increases the resolution at the cost of
 * memory usage and performance. The current implementation internally allocates
 * four long[] whose length are each equal to the number of intervals.
 * 
 * <p>
 * This implementation relies on the use of {@link System#nanoTime()} as an
 * underlying clock.
 * 
 * @author dbaggott
 */
public class RollingAverage {

	/** length of time that the rolling average is made over */
	private final long windowInMillis;

	/** resolution at which data is collected/discarded */
	private final long intervalInMillis;

	/** resolution at which data is collected/discarded */
	private final long intervalInNanos;

	/** sum of all data points for each window interval */
	private final long[] sums;

	/** number of data points recorded in each window interval */
	private final long[] counts;

	/** min data point for each window interval */
	private final long[] mins;

	/** max data point for each window interval */
	private final long[] maxs;

	/** total sum of all values across all window intervals */
	private long sumsTotal;

	/** total number of data points across all window intervals */
	private long countsTotal;

	/** window interval currently being added to */
	private int curIndex = 0;

	/**
	 * the (relative) time at which the current interval started holding data
	 * (in nano seconds)
	 */
	private long curBeginTimeInNanos;

	private ReentrantLock lock;

	private RollingAverage(long windowSizeInMillis, long intervalSizeInMillis) {
		if (intervalSizeInMillis > windowSizeInMillis || windowSizeInMillis % intervalSizeInMillis != 0) {
			throw new IllegalArgumentException("The window size must be evenly divisible by the resolution.");
		}

		this.windowInMillis = windowSizeInMillis;
		this.intervalInMillis = intervalSizeInMillis;
		this.intervalInNanos = intervalSizeInMillis * 1000000;
		int intervalCount = (int) (windowSizeInMillis / intervalSizeInMillis);

		sums = new long[intervalCount];
		counts = new long[intervalCount];
		mins = new long[intervalCount];
		maxs = new long[intervalCount];

		sumsTotal = 0;
		countsTotal = 0;
		curBeginTimeInNanos = System.nanoTime();
		lock = new ReentrantLock();
	}

	/**
	 * Instantiates a {@link RollingAverage} instance for measuring rolling
	 * averages of numerical data over a specified time window. The time window
	 * of interest is subdivided into intervals (buckets) into which data is
	 * collected. The number of intervals determines the accuracy at which data
	 * is measured -- all data added within a given interval "falls out" of the
	 * window as a group.
	 * 
	 * <p>
	 * As an extreme example, consider a rolling average that was being
	 * calculated over a 60 second window with twelve 5 second intervals. At t =
	 * 59, the average would reflect data from t = 1 to 59. However, at t = 62,
	 * the average would reflect data from t = 6 to 62 as data from the first 5
	 * second interval would have been discarded.
	 * 
	 * <p>
	 * Increasing the number of intervals increases the resolution at the cost
	 * of memory usage and performance. The current implementation internally
	 * allocates four long[] whose length are each equal to the number of
	 * intervals.
	 * 
	 * <p>
	 * NOTE: if <code>windowSizeInMillis</code> % <code>intervalNumber</code> !=
	 * 0, the window size will be adjusted as follows:
	 * 
	 * <p>
	 * <code>
	 * long intervalSize = Math.round(windowSizeInMillis / (double) numOfIntervals);<br>
	 * long adjustedWindow = numOfIntervals * intervalSize;
	 * </code>
	 * 
	 * <p>
	 * For values where windowSizeInMillis far exceeds the requested
	 * intervalNumber, the effect will be small. However, to avoid this
	 * possibility use
	 * {@link RollingAverage#getInstanceByIntervalSize(long, long)}.
	 * 
	 * @param windowSizeInMillis
	 *            the desired (see note) length of time (in milliseconds) over
	 *            which rolling averages should be calculated
	 * @param numOfIntervals
	 *            the number of intervals that the window should be subdivided
	 *            into
	 */
	public static RollingAverage getInstanceByIntervalCount(long windowSizeInMillis, int numOfIntervals) {
		if (numOfIntervals <= 0) {
			throw new IllegalArgumentException("Interval number must be a positive whole number.");
		}
		// get the closest interval size based on requested windowSize &
		// intervalNumber
		long intervalSize = Math.round(windowSizeInMillis / (double) numOfIntervals);
		long adjustedWindow = numOfIntervals * intervalSize;
		return getInstanceByIntervalSize(adjustedWindow, intervalSize);
	}

	/**
	 * Instantiates a {@link RollingAverage} instance for measuring rolling
	 * averages of numerical data over a specified time window. The time window
	 * of interest is subdivided into intervals (buckets) into which data is
	 * collected. The number of intervals determines the accuracy at which data
	 * is measured -- all data added within a given interval "falls out" of the
	 * window as a group.
	 * 
	 * <p>
	 * As an extreme example, consider a rolling average that was being
	 * calculated over a 60 second window with twelve 5 second intervals. At t =
	 * 59, the average would reflect data from t = 1 to 59. However, at t = 62,
	 * the average would reflect data from t = 6 to 62 as data from the first 5
	 * second interval would have been discarded.
	 * 
	 * <p>
	 * Increasing the number of intervals increases the resolution at the cost
	 * of memory usage and performance. The current implementation internally
	 * allocates four long[] whose length are each equal to the number of
	 * intervals.
	 * 
	 * @param windowSizeInMillis
	 *            length of time (in milliseconds) over which rolling averages
	 *            should be calculated
	 * @param intervalSizeInMillis
	 *            the interval size (in milliseconds) that the window should be
	 *            subdivided into
	 * @throws IllegalArgumentException
	 *             if the <code>windowSizeInMillis</code> is not evenly
	 *             divisible by the intervalSizeInMillis
	 */
	public static RollingAverage getInstanceByIntervalSize(long windowSizeInMillis, long intervalSizeInMillis) {
		return new RollingAverage(windowSizeInMillis, intervalSizeInMillis);
	}

	/**
	 * Resets all data in pool
	 */
	public void reset() {
		try {
			lock.lock();
			Arrays.fill(sums, 0);
			Arrays.fill(counts, 0);
			Arrays.fill(mins, 0);
			Arrays.fill(maxs, 0);
			sumsTotal = 0;
			countsTotal = 0;
			curBeginTimeInNanos = System.nanoTime();
		} finally {
			lock.unlock();
		}
	}

	/**
	 * Adds specified value to data pool.
	 */
	public void add(long value) {
		try {
			lock.lock();
			reconcileExistingData();
			sums[curIndex] += value;
			sumsTotal += value;
			if (value > maxs[curIndex] || counts[curIndex] == 0)
				maxs[curIndex] = value;
			if (value < mins[curIndex] || counts[curIndex] == 0)
				mins[curIndex] = value;

			// must do counts last
			counts[curIndex]++;
			countsTotal++;
		} finally {
			lock.unlock();
		}
	}

	/**
	 * Note: the accuracy of this value is constrained by the window's interval
	 * size ({@link RollingAverage#getIntervalSize()}) and by the accuracy of
	 * {@link System#nanoTime()}
	 * 
	 * @return the (rounded) average of data values added within the last
	 *         <code>windowSize</code> milliseconds
	 */
	public long getAverage() {
		return Math.round(getAverageAsDouble());
	}

	/**
	 * Note: the accuracy of this value is constrained by the window's interval
	 * size ({@link RollingAverage#getIntervalSize()}) and by the accuracy of
	 * {@link System#nanoTime()}
	 * 
	 * @return the average of data values added within the last
	 *         <code>windowSize</code> milliseconds
	 */
	public double getAverageAsDouble() {
		try {
			lock.lock();
			reconcileExistingData();
			return countsTotal == 0 ? countsTotal : (double) sumsTotal / countsTotal;
		} finally {
			lock.unlock();
		}
	}

	/**
	 * Note: the accuracy of this value is constrained by the window's interval
	 * size ({@link RollingAverage#getIntervalSize()}) and by the accuracy of
	 * {@link System#nanoTime()}
	 * 
	 * @return the sum of data values added within the last
	 *         <code>windowSize</code> milliseconds
	 */
	public long getSum() {
		try {
			lock.lock();
			reconcileExistingData();
			return sumsTotal;
		} finally {
			lock.unlock();
		}
	}

	/**
	 * Minimum data value within the window.
	 * 
	 * <p>
	 * A value of <code>0L</code> indicates that either the max value added
	 * was <code>0L</code> or that no data was added. These two possibilities
	 * can be differentiated by checking whether
	 * {@link RollingAverage#getCount()} == 0.
	 * 
	 * <p>
	 * Note: the accuracy of this value is constrained by the window's interval
	 * size ({@link RollingAverage#getIntervalSize()}) and by the accuracy of
	 * {@link System#nanoTime()}
	 * 
	 * @return the minimum data value added within the last
	 *         <code>windowSize</code> milliseconds
	 */
	public long getMin() {
		try {
			lock.lock();
			reconcileExistingData();
			if (countsTotal == 0)
				return 0;
			
			long windowMin = Long.MAX_VALUE;
			for (int i = 0; i < mins.length; i++) {
				if (mins[i] < windowMin && counts[i] != 0)
					windowMin = mins[i];
			}
			return windowMin;
		} finally {
			lock.unlock();
		}
	}

	/**
	 * Maximum data value within the window.
	 * 
	 * <p>
	 * A value of <code>0L</code> indicates that either the max value added
	 * was <code>0L</code> or that no data was added. These two possibilities
	 * can be differentiated by checking whether
	 * {@link RollingAverage#getCount()} == 0.
	 * 
	 * <p>
	 * Note: the accuracy of this value is constrained by the window's interval
	 * size ({@link RollingAverage#getIntervalSize()}) and by the accuracy of
	 * {@link System#nanoTime()}
	 * 
	 * @return the maximum data value added within the last
	 *         <code>windowSize</code> milliseconds
	 */
	public long getMax() {
		try {
			lock.lock();
			reconcileExistingData();
			if (countsTotal == 0)
				return 0;
			
			long windowMax = 0;
			for (long max : maxs) {
				if (max > windowMax)
					windowMax = max;
			}
			return windowMax;
		} finally {
			lock.unlock();
		}
	}

	/**
	 * Note: the accuracy of this value is constrained by the window's interval
	 * size ({@link RollingAverage#getIntervalSize()}) and by the accuracy of
	 * {@link System#nanoTime()}
	 * 
	 * @return the number of values added within the last
	 *         <code>windowSize</code> milliseconds
	 */
	public long getCount() {
		try {
			lock.lock();
			reconcileExistingData();
			return countsTotal;
		} finally {
			lock.unlock();
		}
	}

	/**
	 * @return The window size (in milliseconds) over which the rolling average
	 *         is calcuated
	 */
	public long getWindowSize() {
		return windowInMillis;
	}

	/**
	 * @return The interval size (in milliseconds) that the rolling average
	 *         window is subdivided into
	 */
	public long getIntervalSize() {
		return intervalInMillis;
	}

	/**
	 * @return The number of intervals that the rolling average window is
	 *         subdivided into
	 */
	public int getIntervalCount() {
		return sums.length;
	}

	/**
	 * @return The observation data for every interval <b>containing data</b>
	 */
	public Collection<ObservationInterval> getIntervalData() {
		try {
			lock.lock();
			reconcileExistingData();

			ArrayList<ObservationInterval> observations = new ArrayList<ObservationInterval>();
			for (int i = 0; i < sums.length; i++)
				if (counts[i] != 0)
					observations.add(new ObservationInterval(sums[i], counts[i], mins[i], maxs[i]));
			return observations;
		} finally {
			lock.unlock();
		}
	}

	/**
	 * Reconciles any existing data based on the current time -- handles
	 * discarding expired data, updating countsTotal/sumsTotal, and correctly
	 * increments the curIndex value.
	 */
	private void reconcileExistingData() {
		try {
			lock.lock();
			int intervalsToClear = (int) ((System.nanoTime() - curBeginTimeInNanos) / intervalInNanos);

			if (intervalsToClear >= sums.length) {
				reset();
			} else if (intervalsToClear > 0) {
				// clear specified number of intervals beyond the current
				// interval
				for (int i = 0; i < intervalsToClear; i++) {
					curIndex++;
					if (curIndex >= sums.length) {
						curIndex = 0;
					}
					sumsTotal -= sums[curIndex];
					sums[curIndex] = 0;
					countsTotal -= counts[curIndex];
					counts[curIndex] = 0;
					mins[curIndex] = 0;
					maxs[curIndex] = 0;
				}
				// this can be adjusted all at once
				curBeginTimeInNanos += (intervalInNanos * intervalsToClear);
			} // else == 0, in the current interval, no need to do anything
		} finally {
			lock.unlock();
		}
	}

	@Override
	public String toString() {
		try {
			/*
			 * locking is necessary to ensure that the count/sum/avg are
			 * internally consistent
			 */
			lock.lock();
			reconcileExistingData();
			StringBuilder sb = new StringBuilder(getClass().getSimpleName()).append(" (");
			sb.append("window:").append(windowInMillis).append(";");
			sb.append("intervals:").append(sums.length).append(";");
			sb.append("count:").append(countsTotal).append(";");
			sb.append("sum:").append(sumsTotal).append(";");
			sb.append("avg:").append(countsTotal == 0 ? countsTotal : (double) sumsTotal / countsTotal).append(";");
			sb.append(")");
			return sb.toString();
		} finally {
			lock.unlock();
		}
	}

	/**
	 * A tuple for passing observation data.
	 * 
	 * @author bcrawford
	 */
	public class ObservationInterval {

		private long sum, count, min, max;

		public long getCount() {
			return count;
		}

		public long getSum() {
			return sum;
		}

		public long getMin() {
			return min;
		}

		public long getMax() {
			return max;
		}

		public ObservationInterval(long sum, long count, long min, long max) {
			this.sum = sum;
			this.count = count;
			this.min = min;
			this.max = max;
		}
	}
}
