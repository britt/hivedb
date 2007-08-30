package org.hivedb.util;

import java.util.Hashtable;
import java.util.Map;

public class GetOpt {
	Map<String,Key> keys;
	Map<String,String> lastProcessed;
	
	public GetOpt() {
		this.keys = new Hashtable<String, Key>();
	}
	
	public GetOpt(String format) {
		this.keys = extractKeys(format);
	}
	
	public Map<String, String> toMap(String[] argz) {
		Map<String,String> argumentMap = new Hashtable<String, String>();
		Integer unnamedArgumentCount = 0;
		
		for(int i=0; i<argz.length; i++) {
			if(isFlag(argz[i])) {
				if(keys.get(trimFlag(argz[i])).requiresArgument && i != argz.length-1)
					argumentMap.put(trimFlag(argz[i]), argz[++i]);
				else
					argumentMap.put(trimFlag(argz[i]), "");
			} else {
				argumentMap.put(unnamedArgumentCount.toString(), argz[i].trim());
				unnamedArgumentCount++;
			}
		}
		lastProcessed = argumentMap;
		return argumentMap;
	}
	
	public Map<String,String> getLast() {return lastProcessed;}
	
	public boolean validate() {
		return validateArgumentMap(this.lastProcessed, keys);
	}
	
	public static boolean validateArgumentMap(Map<String,String> argz, Map<String,Key> keys) {
		boolean isValid = true;
		for(Key key : keys.values())
			isValid &= key.requiresArgument ? !"".equals(argz.get(key.value)) : "".equals(argz.get(key.value));
		return isValid;
	}
	
	public void add(String value, boolean requiresArgument) {
		this.keys.put(value, new Key(value, requiresArgument));
	}
	
	private Map<String,Key> extractKeys(String format) {
		String[] flags = format.split(",");
		Map<String,Key> keys = new Hashtable<String, Key>();
		for(String s : flags) {
			keys.put(trimFormatString(s),new Key(trimFormatString(s), needsArgument(s)));
		}
		return keys;
	}
	
	public static boolean isFlag(String s) {
		return s.startsWith("-");
	}
	
	private String trimFormatString(String s) {
		return s.trim().replaceAll(":", "");
	}
	
	private boolean needsArgument(String s) {
		return s.trim().endsWith(":");
	}
	
	private String trimFlag(String s) {
		s = s.trim();
		while(isFlag(s)) {
			s = s.replaceFirst("-", "");
		}
		return s;
	}
	
	private class Key {
		private boolean requiresArgument;
		private String value;
	
		public Key(String value, boolean requiresArgument) {
			this.value = value;
			this.requiresArgument = requiresArgument;
		}
	}
}
