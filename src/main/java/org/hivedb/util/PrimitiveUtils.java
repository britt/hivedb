package org.hivedb.util;

public class PrimitiveUtils {
	public static boolean isUndefinedId(Object id) {
		if (id.getClass().equals(int.class)
				|| id.getClass().equals(Integer.class))
			return (Integer) id == 0;
		if (id.getClass().equals(long.class)
				|| id.getClass().equals(Long.class))
			return (Long) id == 0L;
		if (id.getClass().equals(short.class)
				|| id.getClass().equals(Short.class))
			return (Short) id == 0;
		if (id.getClass().equals(String.class))
			return (String)id == "";
		throw new RuntimeException("Type not supported");
	}
}
