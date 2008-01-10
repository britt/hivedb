package org.hivedb.util;

import java.util.Map;

import org.apache.cxf.aegis.type.java5.IgnoreProperty;

public interface MapBacked {
	@IgnoreProperty
	public abstract Map getMap();
}