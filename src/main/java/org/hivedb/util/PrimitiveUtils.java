package org.hivedb.util;

import java.math.BigDecimal;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;


public class PrimitiveUtils {
	
	static Date date;
	static {
		Calendar calendar = GregorianCalendar.getInstance();
		calendar.clear();
		date = calendar.getTime();
	}
	
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
		if (isBigDecimal(clazz))
			return new BigDecimal(0);
		if (isString(clazz))
			return "";
		if (isDate(clazz)) {		
			return date;
		}
			 
		throw new RuntimeException(String.format("Class %s not supported", clazz.getSimpleName()));
	}
	
	
	/**
	 *  Is this class representable as one our supported noncomplex types
	 * @param clazz
	 */
	public static boolean isPrimitiveClass(Class clazz) {
		return isInteger(clazz) || isLong(clazz) || isShort(clazz) || isDouble(clazz) || isFloat(clazz) || isBigDecimal(clazz) || isString(clazz) || isDate(clazz)
			|| isBoolean(clazz) || isClass(clazz) || isObject(clazz);
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
	
	public static boolean isBigDecimal(Class clazz) {
		return clazz.equals(BigDecimal.class);
	}
	
	public static boolean isDate(Class clazz) {
		return clazz.equals(Date.class);
	}
	public static boolean isBoolean(Class clazz) {
		return clazz.equals(boolean.class) || clazz.equals(Boolean.class);
	}
	public static boolean isClass(Class clazz) {
		return clazz.equals(Class.class);
	}
	public static boolean isObject(Class clazz) {
		return clazz.equals(Object.class);
	}
}
