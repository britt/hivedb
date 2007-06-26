package org.hivedb;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import org.testng.annotations.Test;

public class BenchmarkingProxyTest {
	
	@Test
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
	
	@Test
	public void testExceptionBehavior() {
		InterruptedNap nap = new InterruptedNap();
		try {
			nap.execute();
		} catch(Exception e) {
			for(Class c : e.getClass().getInterfaces())
				System.out.println(c.getName());
			System.out.println(e.getClass().getSuperclass().getName());
			assertTrue(RuntimeException.class.isInstance(e));
			assertEquals(NapInterruptionException.class, e.getClass());
		}
	}
	
	class Nap extends BenchmarkingProxy<Boolean> {
		private boolean everythingGoOk = true;
		
		@Override
		protected Boolean doWork() {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
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
		protected Boolean doWork() {
			throw new NapInterruptionException("Why did you wake me up, jerk?");
		}
	}
	
	@SuppressWarnings("serial")
	class NapInterruptionException extends RuntimeException {
		NapInterruptionException(String msg) {
			super(msg);
		}
	}
}
