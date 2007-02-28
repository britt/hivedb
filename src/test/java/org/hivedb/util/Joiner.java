package org.hivedb.util;

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
}
