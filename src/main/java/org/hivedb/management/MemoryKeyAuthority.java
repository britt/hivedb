/**
 * 
 */
package org.hivedb.management;

import org.hivedb.management.KeyAuthority;
import org.hivedb.util.functional.Generator;

public class MemoryKeyAuthority implements KeyAuthorityCreator {
	
	public <T extends Number> KeyAuthority<T> create(Class keySpace, final Class<T> returnType) {
		final Generator incrementor;
		if (returnType.equals(int.class) || returnType.equals(Integer.class))
			 incrementor = new Generator<Integer>() {
				private int i=0;;
				public Integer generate() {
					return ++i;
				}
			};
		else if (returnType.equals(long.class) || returnType.equals(Long.class))
			incrementor = new Generator<Long>() {
				private long i=0;;
				public Long generate() {
					return ++i;
				}
			};
		else
			throw new RuntimeException("Only Integers and Longs are supported");
		
		return new KeyAuthority<T>() {

			public T nextAvailableKey() {
				return increment();
			}
			@SuppressWarnings("unchecked")
			private T increment()
			{
				return (T)incrementor.generate();
			}
		};
	}
}