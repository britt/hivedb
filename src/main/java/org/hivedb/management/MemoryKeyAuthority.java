/**
 * 
 */
package org.hivedb.management;

import org.hivedb.management.KeyAuthority;
import org.hivedb.util.functional.Generator;

public class MemoryKeyAuthority implements KeyAuthorityCreator {
	
	public  KeyAuthority create(Class keySpace, final Class returnType) {
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
		
		return new KeyAuthority() {

			public Object nextAvailableKey() {
				return increment();
			}
			@SuppressWarnings("unchecked")
			private Object increment()
			{
				return incrementor.generate();
			}
		};
	}
}