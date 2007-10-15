package org.hivedb;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import org.apache.commons.lang.time.DateUtils;
import org.hivedb.management.statistics.RollingAverage;
import org.testng.annotations.Test;

public class TestRollingAverage {
	
	@Test
	public void testConstructorAndToString() throws Exception {

		try {
			// window multiple of interval size
			RollingAverage good = RollingAverage.getInstanceByIntervalSize(100, 10);
			assertNotNull(good);
			assertEquals(100, good.getWindowSize());
			assertEquals(10, good.getIntervalSize());
			assertEquals(10, good.getIntervalCount());
		} catch (RuntimeException e1) {
			fail(e1.getMessage());
		}

		try {
			// window not multiple of interval size
			RollingAverage bad = RollingAverage.getInstanceByIntervalSize(100, 37);
			fail("Able to initialize: " + bad.toString());
		} catch (RuntimeException e) {
			// good thing
		}

		// window evenly divisible by number of intervals
		RollingAverage good = RollingAverage.getInstanceByIntervalCount(100, 10);
		assertNotNull(good);
		assertEquals(100, good.getWindowSize());
		assertEquals(10, good.getIntervalSize());
		assertEquals(10, good.getIntervalCount());

		// window not evenly divisible: as per api, window size is adjusted
		// make sure that for "reasonable" values (intervalNumber > 10 &&
		// windowSize > 100 * intervalNumber) the diff is "small"
		double toleratedFractionalDifference = 0.01;
		long[] desiredWindows = new long[] { 83294732974L, 12312313, 1234543143553L, 231422331, 112342, 10001 };
		for (long desired : desiredWindows) {
			for (int i = 10; i < 101; i++) {
				RollingAverage adjusted = RollingAverage.getInstanceByIntervalCount(desired, i);
				assertTrue(
						"Window: " + desired + "; intervals: " + i + "; actual: " + adjusted.getWindowSize(),
						(double) (Math.abs(adjusted.getWindowSize() - desired)) / desired <= toleratedFractionalDifference);
			}
		}
	}

	@Test
	public void testAddAndReset() {

		RollingAverage ra = RollingAverage.getInstanceByIntervalSize(DateUtils.MILLIS_PER_DAY,
				DateUtils.MILLIS_PER_MINUTE);

		// no data
		assertEquals(0.0, ra.getAverageAsDouble());
		assertEquals(0, ra.getAverage());

		// add some data
		ra.add(100);
		assertEquals(100, ra.getAverage());
		assertEquals(100, ra.getSum());
		assertEquals(1, ra.getCount());

		// reset
		ra.reset();
		assertEquals(0, ra.getAverage());
		assertEquals(0, ra.getSum());
		assertEquals(0, ra.getCount());

	}

	@Test
	public void testAverage() throws Exception {
		RollingAverage ra = RollingAverage.getInstanceByIntervalSize(1000, 1);

		// add: 0+1+2+3..+9 = 45
		for (int i = 0, value = 0; i < 10; i++) {
			ra.add(value++);
		}
		assertEquals(45, ra.getSum());
		assertEquals(10, ra.getCount());
		assertEquals(4.5, ra.getAverageAsDouble());
		assertEquals(5, ra.getAverage());
	}

	@Test
	public void testWindowClears() {
		try {
			long window = DateUtils.MILLIS_PER_SECOND;
			long interval = 100;
			RollingAverage ra = RollingAverage.getInstanceByIntervalSize(window, interval);

			// some data
			int fixedValue = 10;
			for (int i = 0; i < 5; i++) {
				ra.add(fixedValue);
			}

			// average should be fixed value
			assertEquals(fixedValue, ra.getAverage());
			assertEquals((double) fixedValue, ra.getAverageAsDouble());

			// data should still be there
			Thread.sleep(interval);
			assertEquals(fixedValue, ra.getAverage());

			// let data fall out
			Thread.sleep(window);
			assertEquals(0, ra.getAverage());

		} catch (InterruptedException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testRollsOut() throws Exception {
		// this test is finicky b/c of inaccuracies of Thread.sleep().

		long window = 1000;
		int numIntervals = 4;
		long resolution = window / numIntervals;
		long wait = resolution;

		RollingAverage ra = RollingAverage.getInstanceByIntervalSize(window, resolution);

		// sleep for resolution/2 so our starting point is in the middle...
		// (otherwise we'll be measuring at the edges of intervals and test will
		// be sensitive to cpu load)
		Thread.sleep(resolution / 2);

		// now add some data to successive windows
		int totalAdded = 0;
		for (int i = 0, value = 0; i < numIntervals; i++) {
			totalAdded += value;
			ra.add(value++);
			assertEquals(totalAdded, ra.getSum());
			if (i != numIntervals - 1) {
				System.nanoTime();
				Thread.sleep(wait);
			}
		}

		// make sure added one item per interval
		assertEquals(numIntervals, ra.getCount());
		assertEquals(totalAdded, ra.getSum());

		// data should fall out in order added
		int sumInWindow = totalAdded;
		int countInWindow = numIntervals;
		for (int i = 0, value = 0; i < numIntervals; i++) {
			System.nanoTime();
			Thread.sleep(wait);
			sumInWindow -= value;
			countInWindow--;
			value++;

			assertEquals("Expect count in window: " + countInWindow,countInWindow, ra.getCount());
			assertEquals("Expect sum in window: " + sumInWindow, sumInWindow, ra.getSum());

		}
	}

//	private RollingAverage shared = RollingAverage.getInstanceByIntervalSize(10000, 100);

//	@Test
//	public void testConcurrentUse() throws Exception {
//
//		// crate a bunch of threads who use the shared RollingAverage
//		Thread[] threads = new Thread[10];
//		for (int i = 0; i < threads.length; i++) {
//			threads[i] = new Thread() {
//				@Override
//				public void run() {
//					while (!interrupted()) {
//						shared.toString();
//						shared.add(1);
//						shared.getCount();
//						shared.add(1);
//						shared.getAverage();
//						shared.add(1);
//						shared.getAverageAsDouble();
//						shared.add(1);
//						shared.getSum();
//						shared.reset();
//						shared.add(1);
//						shared.getIntervalSize();
//						shared.add(1);
//						shared.getWindowSize();
//						shared.add(2);
//						shared.toString();
//						try {
//							Thread.sleep((int) (Math.random() * 2));
//						} catch (InterruptedException e) {
//							// accept interruptions while sleeping
//							return;
//						}
//					}
//				}
//			};
//		}
//
//		// start all threads
//		for (Thread thread : threads) {
//			thread.start();
//		}
//
//		// allow threads to work
//		Thread.sleep(1000);
//
//		// all threads should be interruptable (if not, they are locked in some
//		// way)
//		for (Thread t : threads) {
//			t.interrupt();
//			Thread.sleep(1000);
//			assertTrue("Thread should be dead.", !t.isAlive());
//		}
//
//	}
}
