package org.hivedb.util;

import java.util.Map;

import org.hivedb.util.functional.Maps;

public class Mapper implements MapBacked {
	public Mapper() {
		this.map = Maps.newHashMap();
	}
	private Map map = Maps.newHashMap();
	public Map getMap() {
		if(map == null)
			map = Maps.newHashMap();
		return map;}
	public void setMap(Map map) {this.map = map;}
}
