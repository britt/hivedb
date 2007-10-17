package org.hivedb.util;

import java.util.HashMap;
import java.util.Map;

import org.hivedb.util.functional.Binary;
import org.hivedb.util.functional.Unary;

public class FilteringStringBuilder {
	private StringBuilder s;
	private Map<String, AppendFilter> filters;
	
	public FilteringStringBuilder() {
		this(new StringBuilder());
	}
	
	public FilteringStringBuilder(StringBuilder s) {
		this.filters = new HashMap<String, AppendFilter>();
		this.s = s;
	}
	
	public FilteringStringBuilder append(Object o) {
		s.append(o);
		return this;
	}
	
	public FilteringStringBuilder filter(String name, Object... o) {
		return filters.get(name).apply(this, o);
	}
	
	public FilteringStringBuilder addFilter(String name, AppendFilter filter) {
		this.filters.put(name, filter);
		return this;
	}
	
	public AppendFilter getFilter(String key) {
		return filters.get(key);
	}
	
	public String toString() {
		return s.toString();
	}
	
	public interface AppendFilter {
		public FilteringStringBuilder apply(FilteringStringBuilder s, Object...args);
	}
	
	public static AppendFilter format(final String format) {
		return new AppendFilter(){
			public FilteringStringBuilder apply(FilteringStringBuilder s, Object... args) {
				return s.append(String.format(format, args));
			}};
	}
	
	@SuppressWarnings("unchecked")
	public static AppendFilter getMapEntries(final Map map) {
		return new AppendFilter(){
			public FilteringStringBuilder apply(FilteringStringBuilder s, Object... args) {
				for(Object o : args)
					if(map.containsKey(o))
						s.append(map.get(o));
				return s;
			}};
	}
	
	@SuppressWarnings("unchecked")
	public static AppendFilter getMapEntry(final Map map) {
		return new AppendFilter(){
			public FilteringStringBuilder apply(FilteringStringBuilder s, Object... args) {
				if(args.length == 2) {
					if(map.containsKey(args[0]))
						s.append(String.format(args[1].toString(), map.get(args[0])));
				} else
					if(map.containsKey(args[0]))
						s.append(map.get(args[0]));
				return s;
			}};
	}
	
	public static AppendFilter replace(final String pattern, final String subst) {
		return new AppendFilter(){
			public FilteringStringBuilder apply(FilteringStringBuilder s,Object... args) {
				for(Object o : args)
					s.append(o.toString().replaceAll(pattern, subst));
				return s;
		}};
	}
}
