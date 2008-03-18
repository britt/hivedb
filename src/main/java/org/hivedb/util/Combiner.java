package org.hivedb.util;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.hivedb.util.functional.Atom;
import org.hivedb.util.functional.Filter;
import org.hivedb.util.functional.Predicate;
import org.hivedb.util.functional.Transform;
import org.hivedb.util.functional.Unary;

/// <summary>
/// Performs combination operations on a collection with unique elements. All collections are converted to Set Ts to do computations.
/// The basic equation for the number of combinations is
/// n! / (n-r)! * r where n is the number of items in the list and r is the number in each result set
/// When computing combinations of all sizes the equation is thus
/// Sum r=[1,n] n! / ((n-r)! * r!)
/// </summary>
public class Combiner
{
	/// <summary>
	///  Generate all permutations of the given collection up to the given set size
	/// </summary>
	/// <param name="collection"</param>
	/// <returns></returns>
	public static<T> Collection<Set<T>> generateSets(Collection<T> collection, int maxSetSize)
	{
		return generateSets(new HashSet<T>(collection), maxSetSize);
	}
	public static<T> Collection<Set<T>> generateSets(Set<T> set, final int maxSetSize)
	{
		if (set.size() == 0)
			return Collections.emptyList();
		if (set.size() == 1)
			return Collections.singletonList(set);
		
		final T first = Atom.getFirstOrThrow(set);
		// Create all sets without the first item
		Collection<T> rest = Atom.getRestOrThrow(set);
		Collection<Set<T>> results = generateSets(rest, maxSetSize);

		// Combine three sets of sets
		return Transform.flatten((Collection<Set<T>>)
								Transform.map(new Unary<Set<T>, Set<T>>() { // add first item to all previously generated sets
									public Set<T> f(Set<T> set) {
										Set newSet = new HashSet((Set)set);
										newSet.add(first);
										return newSet;
									}}, Filter.grep(new Predicate<Set<T>>() {
										public boolean f(Set<T> set) {
											return set.size() < maxSetSize;
										}}, results)),
								results,													// previously generated sets 
								Collections.singletonList(Collections.singleton(first)));  // the first item in a set alone
	}	
}
