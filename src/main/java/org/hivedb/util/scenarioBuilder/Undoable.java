package org.hivedb.util.scenarioBuilder;

import java.util.Stack;

/**
 *  Creates an undoable statement. Override the function f() with code that includes one or more anonymous overrides of the internal class Undo. 
 *  Note that the use of the function name f indicates that the class only has one method to override, and so is essentially a functor.
 *  After constructing Undoable call f() and then undo() to run all constructed instances of Undo. Or call cycle() which calls f() followed by undo(). 
 *  
 *  Synopsis:
 *  final Foo foo;
 *  try {
 *  new Undoable() { public void f() throws Exception {
 *  	// store some data that will need undoing
 *  	final Bar bar = foo.getBar();
 *  	foo.setBar(new Bar(3.14))
 *  	new Undo() { public void f() throws Exception
 *  		foo.setBar(bar);
 *  	}
 *  	// add more new Undo() calls here
 *  }}.cycle(); // calls Undoable.f() and then all Undo().f defined inside
 *  } catch (Exception e) { ... }
 *  
 * @author Andy Likuski alikuski@cafepress.com
 *
 */
public abstract class Undoable {
	public abstract void f() throws Exception;
	public void undo() throws Exception
	{
		while (undoStack.size() != 0)
			undoStack.pop().f();
	}
	public void cycle() throws Exception
	{
		f();
		undo();
	}
	Stack<Undo> undoStack = new Stack<Undo>();
	public abstract class Undo
	{
		public Undo()
		{
			undoStack.push(this);
		}
		public abstract void f() throws Exception;
	}
}
