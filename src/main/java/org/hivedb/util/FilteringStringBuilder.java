package org.hivedb.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.hivedb.util.functional.Pair;

public class FilteringStringBuilder {
	private StringBuilder s;
	private Map<String, StringBuilderFilter> filters;
	
	public FilteringStringBuilder() {
		this(new StringBuilder());
	}
	
	public FilteringStringBuilder(StringBuilder s) {
		this.filters = new HashMap<String, StringBuilderFilter>();
		this.s = s;
		this.addFilter("replace", replace());
		this.addFilter("format", format());
	}
	
	public FilteringStringBuilder append(Object o) {
		s.append(o);
		return this;
	}
	
	public FilteringStringBuilder filter(String name, String input, Object... args) {
		return this.append(filters.get(name).apply(input, args));
	}
	
	public FilteringStringBuilder addFilter(String name, StringBuilderFilter filter) {
		this.filters.put(name, filter);
		return this;
	}
	
	public StringBuilderFilter getFilter(String key) {
		return filters.get(key);
	}
	
	public String toString() {
		return s.toString();
	}
	
	public static StringBuilderFilter format() {
		return new StringBuilderFilter() {
			public String apply(String text, Object... args) {
				return !"".equals(text) ? String.format(args[0].toString(), text) : "";
			}};
	}
	
	@SuppressWarnings("unchecked")
	public static StringBuilderFilter getMapEntry(final Map map) {
		return new StringBuilderFilter(){
			public String apply(String text, Object... args) {
				return map.containsKey(text) ? map.get(text).toString() : "";
			}};
	}
	
	public static StringBuilderFilter replace() {
		return new StringBuilderFilter(){
			public String apply(String text, Object... args) {
				return text.replace(args[0].toString(), args[1].toString());
			}};
	}

	public StringBuilderFilterChain chain(String text) {
		return new StringBuilderFilterChain(text);
	}
	
	public interface StringBuilderFilter {
		public String apply(String text, Object... args);
	}
	
	public class StringBuilderFilterChain {
		List<Pair<StringBuilderFilter, Object[]>> chain = new ArrayList<Pair<StringBuilderFilter,Object[]>>();
		String input;
		
		public StringBuilderFilterChain(String input) {
			this.input = input;
		}
		
		public StringBuilderFilterChain add(String name, Object... args) {
			chain.add(new Pair<StringBuilderFilter, Object[]>(filters.get(name), args));
			return this;
		}
		
		public String execute() {
			String output = input;
//			Collections.reverse(chain);
			for(Pair<StringBuilderFilter, Object[]> entry : chain) {
				output = entry.getKey().apply(output, entry.getValue());
			}
			return output;
		}
		
		public FilteringStringBuilder end() {
			return append(execute());
		}
	}
}
