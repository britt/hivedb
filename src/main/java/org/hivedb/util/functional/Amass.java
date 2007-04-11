package org.hivedb.util.functional;


public class Amass {
	public static<C,R> R join(Joiner<C,R> joiner, Iterable<C> iterable, R initialValue)
	{
		R result = null;
		for (C item : iterable) {
			if (result == null) 
				result = initialValue;
			result = joiner.f(item,result);
		}					
		return result;
	}
}
