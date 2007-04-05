package org.hivedb;

import org.hivedb.BenchmarkingProxy;
import org.testng.annotations.Test;
import static org.testng.AssertJUnit.*;

public class BenchmarkingProxyTest {
	
	//@Test
	public void testRuntimeMeasurement() throws Exception {
		Nap nap = new Nap();
		nap.execute();
		assertEquals(100, nap.getRuntimeInMillis());
	}
	
	@Test
	public void testFailure() throws Exception {
		Nap nap = new InterruptedNap();
		try {
		nap.execute();
		} catch(Exception e) {
			
		}
		assertFalse(nap.didYouSleepWell());
		assertTrue(nap.getRuntimeInNanos() > 0);
	}
	
	class Nap extends BenchmarkingProxy<Boolean, Exception> {
		private boolean everythingGoOk = true;
		
		@Override
		protected Boolean doWork() throws Exception {
			Thread.sleep(100);
			return true;
		}

		@Override
		protected void onFailure() {
			everythingGoOk = false;
		}

		@Override
		protected void onSuccess(Boolean output) {
			everythingGoOk = true;
		}
		
		public boolean didYouSleepWell() {
			return everythingGoOk;
		}
	}
	
	class InterruptedNap extends Nap {
		
		@Override
		protected Boolean doWork() throws Exception {
			throw new NapInterruptionException("Why did you wake me up, jerk?");
		}
	}
	
	@SuppressWarnings("serial")
	class NapInterruptionException extends Exception {
		NapInterruptionException(String msg) {
			super(msg);
		}
	}
}
