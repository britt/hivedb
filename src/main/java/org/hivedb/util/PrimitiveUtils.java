package org.hivedb.util;

import org.hivedb.util.functional.Binary;
import org.hivedb.util.classgen.ReflectionTools;

import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Blob;
import java.text.DateFormat;
import java.text.ParseException;
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
			return BigDecimal.ZERO;
		if (isString(clazz))
			return "";
		if (isDate(clazz)) {		
			return date;
		}
			 
		throw new RuntimeException(String.format("Class %s not supported", clazz.getSimpleName()));
	}
	public static Object getPrimitiveEquivalent(Object value) {
		if (isInteger(value.getClass()))
			return (int)(Integer)value;
		if (isLong(value.getClass()))
			return (long)(Long)value;
		if (isShort(value.getClass()))
			return (short)(Short)value;
		if (isDouble(value.getClass()))
			return (double)(Double)value;
		if (isFloat(value.getClass()))
			return (float)(Float)value;
		return value;
	}
	
	public static<T> T getMinValue(Class<T> clazz) {
		if (isInteger(clazz))
			return (T)new Integer(Integer.MIN_VALUE);
		if (isLong(clazz))
			return (T)new Long(Long.MIN_VALUE);
		if (isShort(clazz))
			return (T)new Short(Short.MIN_VALUE);
		if (isDouble(clazz))
			return (T)new Double(Double.MIN_VALUE);
		if (isFloat(clazz))
			return (T)new Float(Float.MIN_VALUE);
			 
		throw new RuntimeException(String.format("Class %s not supported", clazz.getSimpleName()));
	}
	public static<T> T getMaxValue(Class<T> clazz) {
		if (isInteger(clazz))
			return (T)new Integer(Integer.MAX_VALUE);
		if (isLong(clazz))
			return (T)new Long(Long.MAX_VALUE);
		if (isShort(clazz))
			return (T)new Short(Short.MAX_VALUE);
		if (isDouble(clazz))
			return (T)new Double(Double.MAX_VALUE);
		if (isFloat(clazz))
			return (T)new Float(Float.MAX_VALUE);
			 
		throw new RuntimeException(String.format("Class %s not supported", clazz.getSimpleName()));
	}
	
	public static Binary getMinFunction(final Class<?> clazz) {
		
		return new Binary() {
			public Object f(Object t1, Object t2) {
				if (isInteger(clazz))
					return Math.min((Integer)t1, (Integer)t2);
				if (isLong(clazz))
					return Math.min((Long)t1, (Long)t2);
				if (isFloat(clazz))
					return Math.min((Float)t1, (Float)t2);
				if (isDouble(clazz))
					return Math.min((Double)t1, (Double)t2);
				throw new RuntimeException(String.format("Class %s not supported", clazz.getSimpleName()));
		}};
	}
	public static Binary getMaxFunction(final Class<?> clazz) {
		
		return new Binary() {
			public Object f(Object t1, Object t2) {
				if (isInteger(clazz))
					return Math.max((Integer)t1, (Integer)t2);
				if (isLong(clazz))
					return Math.max((Long)t1, (Long)t2);
				if (isFloat(clazz))
					return Math.max((Float)t1, (Float)t2);
				if (isDouble(clazz))
					return Math.max((Double)t1, (Double)t2);
				throw new RuntimeException(String.format("Class %s not supported", clazz.getSimpleName()));
		}};
	}
	
	public static <T> T copyInstance(T instance) {
		if (isInteger(instance.getClass()))
			return (T) new Integer(((Integer)instance).intValue());
		if (isLong(instance.getClass()))
			return (T) new Long(((Long)instance).longValue());
		if (isShort(instance.getClass()))
			return (T) new Short(((Short)instance).shortValue());
		if (isDouble(instance.getClass()))
			return (T) new Double(((Double)instance).doubleValue());
		if (isFloat(instance.getClass()))
			return (T) new Float(((Float)instance).floatValue());
		if (isBigDecimal(instance.getClass())) {		
			return (T) ((BigDecimal)instance).multiply(new BigDecimal(new Integer(1)));
		}
		if (isString(instance.getClass()))
			return (T)new String((String)instance); // copy so we can tell that it changed (even though Strings are immutable)
		if (isDate(instance.getClass())) {		
			return (T) ((Date)instance).clone();
		}
		throw new RuntimeException(String.format("Class %s not supported", instance.getClass().getSimpleName()));
	}
	
	/**
	 *  Convert the given string to the given primitive class
	 * @param clazz
	 * @param string
	 * @return
	 */
	public static<T> T parseString(Class<T> clazz, String string) {
		if (isInteger(clazz))
			return (T) new Integer(Integer.parseInt(string));
		if (isLong(clazz))
			return (T) new Long(Long.parseLong(string));
		if (isShort(clazz))
			return (T) new Short(Short.parseShort(string));
		if (isDouble(clazz))
			return (T) new Double(Double.parseDouble(string));
		if (isFloat(clazz))
			return (T) new Float(Float.parseFloat(string));
		if (isBigDecimal(clazz)) {		
			return (T) new BigDecimal(string);
		}
		if (isString(clazz))
			return (T)new String(string); // copy so we can tell that it changed (even though Strings are immutable)
		if (isDate(clazz)) {		
			try {
				return (T)DateFormat.getDateInstance().parse(string);
			} catch (ParseException e) {
				throw new RuntimeException(e);
			}
		}
		throw new RuntimeException(String.format("Class %s not supported", clazz.getSimpleName()));
	}
	
	/**
	 *  Is this class representable as one our supported noncomplex types
	 * @param clazz
	 */
	public static boolean isPrimitiveClass(Class clazz) {
		return isInteger(clazz) || isLong(clazz) || isShort(clazz) || isDouble(clazz) || isFloat(clazz) || isBigDecimal(clazz) || isString(clazz) || isDate(clazz)
			|| isBoolean(clazz) || isClass(clazz) || isObject(clazz) || isSerializationClass(clazz);
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
		return ReflectionTools.doesImplementOrExtend(clazz, Date.class); // account for java.sql derivatives
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
	public static boolean isSerializationClass(Class clazz) {
		return clazz.equals(Blob.class) || clazz.equals(InputStream.class);
	}
	
}
