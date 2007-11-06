package org.hivedb.util.functional;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 *  Operations on single or multi-dimensional maps which take multi-argument operators. These functions eliminate the need to 
 *  write messy embedded loops for standard navigation through multi-dimensional maps
 * @author Andy
 *
 */
public class Maps {
	
	public static<K,V> Map<K,V> newHashMap() {
		return new HashMap<K, V>();
	}
	
	public static<I1,I2,V,R> Map<I1, Map<I2, Entry<V,R>>> dig(
			final Ternary<I1,I2,V,R> mapper, 
			final Unary<I2,V> getV,
			final Unary<I1,Collection<I2>> getI2Collection,
			final Collection<I1> collection
			) {
		return Transform.toMap(
			new Transform.IdentityFunction<I1>(),
			new Unary<I1, Map<I2, Entry<V,R>>>() {
				public Map<I2, Entry<V,R>> f(final I1 i1) {
					return Transform.toMap(
						new Transform.IdentityFunction<I2>(),
						new Unary<I2, Entry<V,R>>() {
							public Entry<V,R> f(I2 i2) {
								V v = getV.f(i2);
								return new Pair<V,R>(
									v,
									mapper.f(i1, i2, v));
							}},
						getI2Collection.f(i1));				
				}},
			collection);
	}
	
	public static<I1,I2,V,R> Map<I1, Map<I2, Entry<V,R>>> digMap(
			final Ternary<I1,I2,V,R> mapper, 
			Map<I1, Map<I2, V>> map) {
		return Transform.toMap(
			new Transform.MapToKeyFunction<I1,Map<I2, V>>(),
			new Unary<Entry<I1,Map<I2, V>>, Map<I2, Entry<V,R>>>() {
				public Map<I2, Entry<V,R>> f(final Entry<I1,Map<I2, V>> entry1) {
					return Transform.toMap(
						new Transform.MapToKeyFunction<I2,V>(),
						new Unary<Entry<I2, V>, Entry<V,R>>() {
							public Entry<V,R> f(Entry<I2, V> entry2) {
								return new Pair<V,R>(
										entry2.getValue(),
										mapper.f(entry1.getKey(), entry2.getKey(), entry2.getValue()));
							}},
						entry1.getValue().entrySet());				
				}},
			map.entrySet());
	}

	/**
	 *  Given a 2 dimensional map whose last dimension is a collection, performs an operation with the given
	 *  Binary operator.
	 * @param <I1>
	 * @param <I2>
	 * @param <R>
	 * @param mapper Binary operator where the operator is given each combination of I1,I2 and returns R. You will likely
	 *  return I2 for R unless you actually are doing a tranform of each I2 value to R
	 * @param map The 2 dimensional map whose last dimension is a collection of type I3
	 * @return a 2 dimensional map identical to map except with I2 values transformed to R values
	 */
	public static<I1,I2,R> Map<I1, Collection<R>> digMapToCollection(
			final Binary<I1,I2,R> mapper, 
			Map<I1, Collection<I2>> map) {
	
		return Transform.toMap(
			new Transform.MapToKeyFunction<I1,Collection<I2>>(),
			new Unary<Entry<I1,Collection<I2>>, Collection<R>>() {
				public Collection<R> f(final Entry<I1,Collection<I2>> entryI1I2) {
					
					return Transform.map(new Unary<I2,R>() {
						public R f(I2 i2) {
							return mapper.f(entryI1I2.getKey(), i2);		
						}},
						entryI1I2.getValue());										
				}},
			map.entrySet());
	}
	/**
	 *  Given a 3 dimensional map whose last dimension is a collection, performs an operation with the given
	 *  Ternary operator.
	 * @param <I1>
	 * @param <I2>
	 * @param <I3>
	 * @param <R>
	 * @param mapper Ternary operator where the operator is given each combination of I1,I2,I3 and returns R. You will likely
	 *  return I3 for R unless you actually are doing a tranform of each I3 value to R
	 * @param map The 3 dimensional map whose last dimension is a collection of type I3
	 * @return a 3 dimensional map identical to map except with I3 values transformed to R values
	 */
	public static<I1,I2,I3,R> Map<I1, Map<I2, Collection<R>>> digMapToCollection(
			final Ternary<I1,I2,I3,R> mapper, 
			Map<I1, Map<I2, Collection<I3>>> map) {
	
		return Transform.toMap(
			new Transform.MapToKeyFunction<I1,Map<I2,Collection<I3>>>(),
			new Unary<Entry<I1,Map<I2, Collection<I3>>>, Map<I2, Collection<R>>>() {
				public Map<I2, Collection<R>> f(final Entry<I1,Map<I2, Collection<I3>>> entryI1I2) {
					
					return Transform.toMap(
						new Transform.MapToKeyFunction<I2, Collection<I3>>(),
						new Unary<Entry<I2, Collection<I3>>, Collection<R>>() {
							public Collection<R> f(final Entry<I2, Collection<I3>> entryI2I3) {
								
								return Transform.map(new Unary<I3,R>() {
									public R f(I3 i3) {
										return mapper.f(entryI1I2.getKey(), entryI2I3.getKey(), i3);		
									}},
									entryI2I3.getValue());							
							}},
							entryI1I2.getValue().entrySet());				
				}},
			map.entrySet());
	}
	
	/**
	 *  Given a 4 dimensional map whose last dimension is a collection, performs an operation with the given
	 *  Quaternary operator.
	 * @param <I1>
	 * @param <I2>
	 * @param <I3>
	 * @param <I4>
	 * @param <R>
	 * @param mapper Quaternary operator where the operator is given each combination of I1,I2,I3,14 and returns R. You will likely
	 *  return I4 for R unless you actually are doing a tranform of each I4 value to R
	 * @param map The 4 dimensional map whose last dimension is a collection of type I4
	 * @return a 4 dimensional map identical to map except with I4 values transformed to R values
	 */
	public static<I1,I2,I3,I4,R> Map<I1, Map<I2, Map<I3, Collection<R>>>> digMapToCollection(
			final Quaternary<I1,I2,I3,I4,R> mapper, 
			Map<I1, Map<I2, Map<I3, Collection<I4>>>> map) {
	
		return Transform.toMap(
			new Transform.MapToKeyFunction<I1,Map<I2, Map<I3,Collection<I4>>>>(),
			new Unary<Entry<I1,Map<I2, Map<I3,Collection<I4>>>>, Map<I2, Map<I3,Collection<R>>>>() {
				public Map<I2, Map<I3,Collection<R>>> f(final Entry<I1,Map<I2, Map<I3,Collection<I4>>>> entryI1I2) {
					
					return Transform.toMap(
						new Transform.MapToKeyFunction<I2, Map<I3,Collection<I4>>>(),
						new Unary<Entry<I2, Map<I3,Collection<I4>>>, Map<I3,Collection<R>>>() {
							public Map<I3,Collection<R>> f(final Entry<I2, Map<I3,Collection<I4>>> entryI2I3) {
								
								return Transform.toMap(
									new Transform.MapToKeyFunction<I3, Collection<I4>>(),
									new Unary<Entry<I3,Collection<I4>>, Collection<R>>() {
										public Collection<R> f(final Entry<I3,Collection<I4>> entryI3I4) {
											
											return Transform.map(new Unary<I4,R>() {
												public R f(I4 i4) {
													return mapper.f(entryI1I2.getKey(), entryI2I3.getKey(), entryI3I4.getKey(), i4);		
												}},
												entryI3I4.getValue());
									}},
									entryI2I3.getValue().entrySet());								
							}},
						entryI1I2.getValue().entrySet());				
				}},
			map.entrySet());
	}
}
