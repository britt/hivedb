package org.hivedb.util.scenarioBuilder;

import junit.framework.TestCase;
import org.hivedb.util.scenarioBuilder.Undoable;
public class AssertUtils  {
	public static interface Toss {
		void f() throws Exception;
	}
	public static abstract class UndoableToss extends Undoable implements Toss
	{
	}
	/**
	 *  Asserts that the implementation of Toss throws an exception. 
	 * @param toss A functor to wrap code that throws an exception
	 */
	public static void assertThrows(Toss toss)
	{
		try {
			toss.f();
		} catch (Exception e) {
			return;
		}
		TestCase.fail("Expected exception but none occured");
	}
	/**
	 *  Asserts that the implementation of Toss throws an exception of the given type 
	 * @param toss A functor to wrap code that throws an exception
	 */
	public static void assertThrows(Toss toss, Class<? extends Exception> exceptionType)
	{
		try {
			toss.f();
		} catch (Exception e) {
			if (e.getClass().equals(exceptionType)) // Should check inheritance and implements also
				return;
			throw new RuntimeException("Expected exception of type " + exceptionType.getName() + " but got exception of type " + e.getClass().getName(), e);
		}
		TestCase.fail("Expected exception of type " + exceptionType.getName() + " but no exception occured occured");
	}
	/**
	 *  Asserts that the implementation of UndoablToss throws an exception of the given type, and then calls all the Undo functors created in UndoableToss.
	 *  Calls TestCase.fail() if no exception is thrown
	 *  Throws a RuntimeException if an exception occurs in one of the Undo.f() calls.
	 *  Synopsis:
	 *  final Bar bar = foo.getBar();
	 *  assertThrows(new UndoableToss() { public void f() throws Exception { // calls UndoableToss().f, catches the exception, and then calls all Undo.f()s defined.
	 *  	final String name = foo.getName();
	 *  	new Undo() { public void f() throws Exception { // construct the Undo before the expected exception
	 *  		foo.setName(name);
	 *  	}};
	 *  	foo.setBar(invalid data); // expects a throw
	 *  }});
	 * @param toss A functor to wrap code that throws an exception
	 */
	public static void assertThrows(UndoableToss toss)
	{
		try {
			toss.f();
		} catch (Exception e) {
			try {
				toss.undo();
				return;
			}
			catch (Exception ue) {
				new RuntimeException("Got initial expected exception but got unexpected exception calling undo", ue);
			}
		}
		TestCase.fail("Expected exception but none occured");
	}
	public static void assertThrows(UndoableToss toss, Class<? extends Exception> exceptionType)
	{
		try {
			toss.f();
		} catch (Exception e) {
			try {
				if (e.getClass().equals(exceptionType)) // Should check inheritance and implements also
					toss.undo();
				else
					throw new RuntimeException("Expected exception of type " + exceptionType.getName() + " but got exception of type " + e.getClass().getName(), e);
				return;
			}
			catch (Exception ue) {
				throw new RuntimeException("Got initial expected exception but got unexpected exception calling undo", ue);
			}
		}
		TestCase.fail("Expected exception but none occured");
	}
}
