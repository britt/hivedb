package org.hivedb.util;

public class PrimitiveUtils {
	public static boolean isUndefinedId(Object id) {
		return getUndefinedValue(id.getClass()).equals(id);
	}
	public static Object getUndefinedValue(Class clazz) {
		if (isInteger(clazz))
			return 0;
		if (isLong(clazz))
			return 0L;
		if (isShort(clazz))
			return (short) 0;
		if (isDouble(clazz))
			return (double) 0;
		if (isFloat(clazz))
			return (float) 0;
		if (isString(clazz))
			return "";
		throw new RuntimeException(String.format("Class %s not supported", clazz.getSimpleName()));
	}
	
	/**
	 *  Is this class representable as one our supported noncomplex types
	 * @param clazz
	 */
	public static boolean isPrimitiveClass(Class clazz) {
		return isInteger(clazz) || isLong(clazz) || isShort(clazz) || isDouble(clazz) || isFloat(clazz) || isString(clazz);
	}
	
	public static boolean isString(Class clazz) {
		return clazz.equals(String.class);
	}
	
	public static boolean isLong(Class clazz) {
		return clazz.equals(long.class)
				|| clazz.equals(Long.class);
	}
	public static boolean isInteger(Class clazz) {
		return clazz.equals(int.class)
				|| clazz.equals(Integer.class);
	}
	public static boolean isShort(Class clazz) {
		return clazz.equals(short.class)
				|| clazz.equals(Short.class);
	}
	public static boolean isDouble(Class clazz) {
		return clazz.equals(double.class)
		|| clazz.equals(Double.class);
	}
	public static boolean isFloat(Class clazz) {
		return clazz.equals(float.class)
		|| clazz.equals(Float.class);
	}
}
