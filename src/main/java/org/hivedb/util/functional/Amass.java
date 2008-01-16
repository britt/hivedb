package org.hivedb.util.functional;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import org.hivedb.util.PrimitiveUtils;
import org.hivedb.util.ReflectionTools;
import org.hivedb.util.database.test.WeatherReport;

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
	
	public static<C,R> R min(final Unary<C,R> unary, Iterable<C> iterable, final Class<R> primitiveType)
	{
		return Amass.join(
				new Joiner<C, R>() {
					public R f(C c, R result) {
						return (R) PrimitiveUtils.getMinFunction(primitiveType).f(result, unary.f(c));
					}},
				iterable,
				PrimitiveUtils.getMinValue(primitiveType));
	}
	
	public static<C,R> R max(final Unary<C,R> unary, Iterable<C> iterable, final Class<R> primitiveType)
	{
		return Amass.join(
				new Joiner<C, R>() {
					public R f(C c, R result) {
						return (R) PrimitiveUtils.getMaxFunction(primitiveType).f(result, unary.f(c));
					}},
				iterable,
				PrimitiveUtils.getMaxValue(primitiveType));
	}
	
	/**
	 *  Joins with the given function. The initial value is the first item of the iterable
	 *  Subsequent items of the iterable will be passed the join function along with the previous result of joiner or the initial value
	 * @param <C>
	 * @param joiner
	 * @param iterable
	 * @return
	 */
	public static<C> C join(Joiner<C,C> joiner, Iterable<C> iterable)
	{
		C result = null;
		for (C item : iterable) {
			result = (result == null) 
				? item
				: joiner.f(item,result);
		}					
		return result;
	}
	
	public static<C> String joinByToString(Joiner<C,String> joiner, Iterable<C> iterable)
	{
		String result = "";
		for (C item : iterable) {
			result = (result == null) 
				? item.toString()
				: joiner.f(item,result);
		}					
		return result;
	}
	
	/**
	 * 
	 * Joins with the given join function. The initial value is that of firstCall with the first item of the iterable passed to it.
	 * Subsequent items of the iterable will be passed the join function along with the previous result of firstCall or joiner
	 * 
	 * @param <C>
	 * @param <R>
	 * @param joiner
	 * @param firstCall
	 * @param iterable
	 * @return
	 */
	public static<C,R> R join(Joiner<C,R> joiner, Unary<C,R> firstCall, Iterable<C> iterable)
	{
		R result = null;
		for (C item : iterable) {
			result = (result == null) 
				?firstCall.f(item)
				:joiner.f(item,result);
		}					
		return result;
	}
	
	/**
	 *  Merges two equal size collections
	 * @param <C1>
	 * @param <C2>
	 * @param <R>
	 * @param merger
	 * @param iterable1
	 * @param iterable2
	 * @param initialValue
	 * @return
	 */
	public static<C1,C2,R> R merge(Merger<C1,C2,R> merger, Iterable<C1> iterable1, Iterable<C2> iterable2, R initialValue)
	{
		R result = null;
		Iterator<C1> iterator1 = iterable1.iterator();
		Iterator<C2> iterator2 = iterable2.iterator();
		while (iterator1.hasNext() && iterator2.hasNext()) {
			if (result == null) 
				result = initialValue;
			result = merger.f(iterator1.next(), iterator2.next(), result);
		}					
		return result;
	}
	
	/**
	 *  Perform 3 possible actions on depending on what set the merged itemes fall into.
	 * @param <T>
	 * @param leftOnly - run on items only in leftIterable
	 * @param both - run on items in both
	 * @param rightOnly - run on items only in rightIterable
	 * @param leftIterable - "left" items to merge
	 * @param rightIterable - "right" items to merge
	 * @return 3 collections of the resulting items returned by the Unary functions
	 */
	@SuppressWarnings("unchecked")
	public static <T> MergeResult mergeTerciary(Unary<T,T> leftOnly, Unary<T,T> both, Unary<T,T> rightOnly, Iterable<T> leftIterable, Iterable<T> rightIterable)
	{
		// this should be optimized if ever used on large sets (i.e. O(n) not 3*O(n^2) )
		return new MergeResult<T>(
				Transform.map(leftOnly, Filter.grepFalseAgainstList(rightIterable, leftIterable)),
				Transform.map(both, Filter.grepAgainstList(rightIterable, leftIterable)),
				Transform.map(rightOnly, Filter.grepFalseAgainstList(leftIterable, rightIterable)));
	}
	public static class MergeResult<T>
	{
		Collection<T> leftOnly, both, rightOnly;
		public MergeResult(Collection<T> leftOnly, Collection<T> both, Collection<T> rightOnly) {
			super();
			this.leftOnly = leftOnly;
			this.both = both;
			this.rightOnly = rightOnly;
		}
		public Collection<T> getBoth() {
			return both;
		}

		public Collection<T> getLeftOnly() {
			return leftOnly;
		}

		public Collection<T> getRightOnly() {
			return rightOnly;
		}
	}
	
	public static int makeHashCode(Object[] objects) {
		return makeHashCode(Arrays.asList(objects));
	}
	public static int makeHashCode(Collection<Object> objects) {
		final String join = Amass.join(
				new Joiner.ConcatHashCodesOfValues(),
				new Unary<Object, String>() {
					public String f(Object item) {
						int hash;
						if(ReflectionTools.doesImplementOrExtend(item.getClass(), Collection.class))
							hash = new HashSet<Object>((Collection<Object>) item).hashCode();
						else
							hash = item.hashCode();
						return new Integer(hash).toString();
					}
				},
				objects);
		return join != null ? join.hashCode() : 0;
	}

	@SuppressWarnings("unchecked")
	static public <K, V> Map<K, V> concatOrderedMaps(Map<K,V> baseMap, Map<K,V> appendMap) {
		return Transform.toOrderedMap(Transform.flatten(new Collection[] {
			baseMap.entrySet(),
			appendMap.entrySet()
		}));
	}
	
	@SuppressWarnings("unchecked")
	static public <T> Collection<T> concatCollections(Collection<T> baseCollection, Collection<T> appendCollection) {
		return Transform.flatten(new Collection[] {
			baseCollection,
			appendCollection
		});
	}
}
