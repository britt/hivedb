package org.hivedb.util.functional;

public abstract class Joiner<C,R> {
	public abstract R f(C item, R result);
	
	public static class ConcatStrings<C> extends Joiner<C, String>
	{
		String separator;
		public ConcatStrings(String separator)
		{
			this.separator = separator;
		}
		public String f(C item, String result) {
			return result + separator + item.toString();
		}		
	}
	
	/**
	 *  A Joiner that appends each item's hash code to the running total. Null items are ignored
	 * @author andylikuski
	 *
	 */
	public static class ConcatHashCodesOfValues extends Joiner<Object, String>
	{
		public String f(Object item, String result) {
			return result + (item != null ? new Integer(item.hashCode()).toString() : "");
		}		
	}
}
