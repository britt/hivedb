package org.hivedb.util;

import java.util.Map;

import org.hivedb.util.functional.Maps;

public class Mapper implements MapBacked {
	private Map map;
	public Map getMap() {
		if(map == null)
			map = Maps.newHashMap();
		return map;}
	public void setMap(Map map) {this.map = map;}
}
