package org.hivedb.util.scenarioBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

public abstract class Filter {
	/**
	 *  Extend Filter to make an anoymouse filter function
	 * @param <T>
	 * @param iterable
	 * @return
	 */
	public abstract<T> Collection<T> f(Iterable<T> iterable);
	
	public static<T> Collection<T> grep(Predicate<T> filterer, Iterable<T> iterable)
	{
		List<T> list = new ArrayList<T>();
		for (T item : iterable)
			if (filterer.f(item))
				list.add(item);
		return list;
	}
	public static<T> Collection<T> grep(Predicate<T> filterer, T[] array)
	{
		return grep(filterer, Arrays.asList(array));
	}
	
	public static<T> T grepSingle(Predicate<T> filterer, Iterable<T> iterable) throws NoSuchElementException
	{
		return getFirst(grep(filterer, iterable));
	}
	public static<T> T grepSingle(Predicate<T> filterer, T[] array)
	{
		return grepSingle(filterer, Arrays.asList(array));
	}
	
	public static<T> T getFirst(Iterable<T> iterable) throws NoSuchElementException
	{
		Iterator<T> iterator = iterable.iterator();
		if (iterator.hasNext())
			return iterator.next();
		throw new NoSuchElementException("No first element of the iterable exists");
	}
	public static Boolean hasFirst(Iterable iterable)
	{
		return iterable.iterator().hasNext();
	}
	
	public static<T> T getFirst(T[] array) throws NoSuchElementException
	{
		if (array.length > 0)
			return array[0];
		throw new NoSuchElementException("No first element of the array exists");
	}
	public static Boolean hasFirst(Object[] array)
	{
		return array.length > 0;
	}
	
	public static<T> Boolean isMatch(final Predicate<T> filterer, Iterable<T> iterable)
	{
		for (T item : iterable)
			if (filterer.f(item))
				return true;
		return false;
	}
	public static<T> Boolean isMatch(final Predicate<T> filterer, T[] array)
	{
		return isMatch(filterer, Arrays.asList(array));
	}
	
	// Passes through all superset items that match a subset item
	// The match is determined by calling the equals method of either the superset or subset item.
	// Therefore the type of both Iterable must match (probably need wildcards here actually)
	// If you need to pass in an array, use Arrays.asList() to make it iterable
	public static<T> Collection<T> grepAgainstList(final Iterable<T> subset, Iterable<T> superset)
	{
		Predicate<T> doesThisSupersetItemMatchAnySubsetItem = makeDoesSupersetItemMatchAnySubsetItem(subset, new EqualFunction<T>());
		return grep(doesThisSupersetItemMatchAnySubsetItem, superset);
	}
	public static<T> boolean grepItemAgainstList(final T subItem, Iterable<T> superset)
	{
		List<T> list = new ArrayList<T>(1);
		list.add(subItem);
		Predicate<T> doesThisSupersetItemMatchAnySubsetItem = makeDoesSupersetItemMatchAnySubsetItem(list, new EqualFunction<T>());
		return isMatch(doesThisSupersetItemMatchAnySubsetItem, superset);
	}
	// Passes through all superset items that match a subset item
	// The match is determined by passing all permutations needed of subset and superset item to CompareFunction, which returns true to indicate a match and false otherwise.
	// Using Grep.EqualsFunction is the same as not passing in any CompareFunction
	// If you need to pass in an array, use Arrays.asList() to make it iterable
	public static<SUP,SUB> Collection<SUP> grepAgainstList(final Iterable<SUB> subset, Iterable<SUP> superset, CompareFunction<SUB, SUP> compareFunction)
	{
		Predicate<SUP> doesThisSupersetItemMatchAnySubsetItem = makeDoesSupersetItemMatchAnySubsetItem(subset, compareFunction);
		return grep(doesThisSupersetItemMatchAnySubsetItem, superset);
	}
	public static<SUP,SUB> boolean grepItemAgainstList(final SUB subItem, Iterable<SUP> superset, CompareFunction<SUB, SUP> compareFunction)
	{
		List<SUB> list = new ArrayList<SUB>(1);
		list.add(subItem);
		Predicate<SUP> doesThisSupersetItemMatchAnySubsetItem = makeDoesSupersetItemMatchAnySubsetItem(list,compareFunction);
		return isMatch(doesThisSupersetItemMatchAnySubsetItem, superset);
	}
	
	public static<T> Collection<T> grepFalseAgainstList(final Iterable<T> subset, Iterable<T> superset)
	{
		Predicate<T> doesThisSupersetItemNotMatchAnySubsetItem = makeDoesSupersetItemNotMatchAnySubsetItem(subset, new EqualFunction<T>());
		return grep(doesThisSupersetItemNotMatchAnySubsetItem, superset);
	}
	public static<T> Collection<T> grepItemFalseAgainstList(final T subItem, Iterable<T> superset)
	{
		List<T> list = new ArrayList<T>();
		list.add(subItem);
		Predicate<T> doesThisSupersetItemNotMatchAnySubsetItem = makeDoesSupersetItemNotMatchAnySubsetItem(list, new EqualFunction<T>());
		return grep(doesThisSupersetItemNotMatchAnySubsetItem, superset);
	}
	public static<SUP,SUB> Collection<SUP> grepFalseAgainstList(final Iterable<SUB> subset, Iterable<SUP> superset, CompareFunction<SUB, SUP> compareFunction)
	{
		Predicate<SUP> doesThisSupersetItemNotMatchAnySubsetItem = makeDoesSupersetItemNotMatchAnySubsetItem(subset, compareFunction);
		return grep(doesThisSupersetItemNotMatchAnySubsetItem, superset);
	}
	public static<SUP,SUB> Collection<SUP> grepItemFalseAgainstList(final SUB subItem, Iterable<SUP> superset, CompareFunction<SUB, SUP> compareFunction)
	{
		List<SUB> list = new ArrayList<SUB>();
		list.add(subItem);
		Predicate<SUP> doesThisSupersetItemNotMatchAnySubsetItem = makeDoesSupersetItemNotMatchAnySubsetItem(list, compareFunction);
		return grep(doesThisSupersetItemNotMatchAnySubsetItem, superset);
	}
	
	// Returns true if all superset items match a subset item
	// The match is determined by callin the equals method of either the superset or subset item
	// If you need to pass in an array, use Arrays.asList() to make it iterable
	public static<T> Boolean allMatch(final Iterable<T> subset, Iterable<T> superset)
	{
		Predicate<T> doesThisSupersetItemMatchAnySubsetItem = makeDoesSupersetItemMatchAnySubsetItem(subset, new EqualFunction<T>());
		for (T supersetItem : superset)
			if (!doesThisSupersetItemMatchAnySubsetItem.f(supersetItem))
				return false;
		return true;
	}
	
	
	private static <SUB,SUP> Predicate<SUP> makeDoesSupersetItemMatchAnySubsetItem(final Iterable<SUB> subset, final CompareFunction<SUB, SUP> compareFunction) {
		Predicate<SUP> doesThisSupersetItemMatchAnySubsetItem = new Predicate<SUP>()
		{
			public boolean f(final SUP supersetItem)
			{
				Predicate<SUB> doesEnlosedSupersetItemMatchThisSubsetItem = new Predicate<SUB>()
				{
					public boolean f(final SUB subsetItem) {
						return compareFunction.f(subsetItem, supersetItem);
					}
				};
				return isMatch(doesEnlosedSupersetItemMatchThisSubsetItem,subset);
			}
		};
		return doesThisSupersetItemMatchAnySubsetItem;
	}
	private static <SUB,SUP> Predicate<SUP> makeDoesSupersetItemNotMatchAnySubsetItem(final Iterable<SUB> subset, final CompareFunction<SUB, SUP> compareFunction) {
		Predicate<SUP> doesThisSupersetItemMatchAnySubsetItem = new Predicate<SUP>()
		{
			public boolean f(final SUP supersetItem)
			{
				Predicate<SUB> doesEnlosedSupersetItemMatchThisSubsetItem = new Predicate<SUB>()
				{
					public boolean f(final SUB subsetItem) {
						return compareFunction.f(subsetItem, supersetItem);
					}
				};
				return !isMatch(doesEnlosedSupersetItemMatchThisSubsetItem,subset);
			}
		};
		return doesThisSupersetItemMatchAnySubsetItem;
	}
	
	public static<T> Collection<T> getUnique(Iterable<T> iterable)
	{
		Map<T,Boolean> map = new Hashtable<T,Boolean>();
		for (T item : iterable)
			map.put(item, true);
		return map.keySet();
	}
	public static class TruePredicate implements Predicate 
	{
		public boolean f(Object item) {
			return true;
		}
	}
	public static class FalsePredicate implements Predicate 
	{
		public boolean f(Object item) {
			return false;
		}
	}

	public static class UniquePredicate<T> implements Predicate<T>
	{
		private Map<T, Boolean> map = new Hashtable<T, Boolean>();
		public boolean f(T item) {
			if (map.containsKey(item))
				return false;
			map.put(item, true);
			return true;
		}
	}
	public static class FirstOnlyFilter extends Filter
	{
		public <T> Collection<T> f(Iterable<T> iterable) {
			return grep(new FirstOnlyPredicate<T>(), iterable);
		}
	}
	public static class FirstOnlyPredicate<T> implements Predicate<T>
	{
		private boolean first;
		public boolean f(T item) {
			if (first)
				return false;
			first = true;
			return true;
		}
	}
	public static class AllowAllFilter extends Filter
	{
		@SuppressWarnings("unchecked")
		public <T> Collection<T> f(Iterable<T> iterable) {
			return (Collection<T>)grep(new TruePredicate(), iterable);
		}
	}
	public static class BlockAllFilter extends Filter
	{
		public <T> Collection<T> f(Iterable<T> iterable) {
			return new ArrayList<T>();
		}
	}
	public static<T> Collection<T> removeItemAtIndex(int index, Collection<T> collection) {
		List<T>list =  new ArrayList<T>(collection);
		list.remove(index);
		return list;
	}
	public static<T> Object getItemAtIndex(int index, Collection<T> collection) {
		return new ArrayList<T>(collection).get(index);
	}
	
	public static abstract class CompareFunction<T,S> {public abstract boolean f(final T item1, S item2 );}
	
	
	public static class EqualFunction<T> extends CompareFunction<T,T>
	{
		public boolean f(final Object item1, Object item2 )
		{
			return item1.equals(item2);
		}
	}
	
	public static class UnequalFunction<T> extends CompareFunction<T,T>
	{
		public boolean f(final T item1, T item2 )
		{
			return !item1.equals(item2);
		}
	}

}

    

