package org.hivedb.util.functional;

import java.sql.Timestamp;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;

import org.hivedb.util.ReflectionTools;

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
			return (result != null ? result : "") + hashItem(item);
		}
	}
	public static String hashItem(Object item) {
		int hash;
		if (item == null)
			return "";
		else if(ReflectionTools.doesImplementOrExtend(item.getClass(), Collection.class))
			hash = new HashSet<Object>((Collection<Object>) item).hashCode();
		else if (item instanceof Date)
			hash = new Long(((Date)item).getTime()).hashCode(); // fixes format hashing problems
		else if (item instanceof Timestamp)
			hash = new Long(((Timestamp)item).getTime()).hashCode(); // fixes format hashing problems
		else
			hash = item.hashCode();
		return "" + hash;
	}
}
