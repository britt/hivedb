package org.hivedb.util;

import java.util.Map;

public interface GeneratedImplementation {
	/**
	 *  Retrieve the primary interface implemented by this proxy
	 * @return
	 */
	Class getUnderlyingInterface();
	/**
	 *  Get the underlying Map that stores the field data of the proxy implementation
	 * @return
	 */
	Map getMap();
}
