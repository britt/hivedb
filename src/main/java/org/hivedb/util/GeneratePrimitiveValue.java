package org.hivedb.util;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Random;

import org.hivedb.util.functional.Generator;

public class GeneratePrimitiveValue<F> implements Generator<F> {

	Class<F> clazz;
	Random random;
	public GeneratePrimitiveValue(Class<F> clazz)
	{
		this.clazz = clazz;
		this.random = new Random();
	}
	
	@SuppressWarnings("unchecked")
	/**
	 *  Generate random values that are zero or positive when applicable
	 */
	public F generate() {
		if (PrimitiveUtils.isInteger(clazz))
			return (F)new Integer(Math.abs(random.nextInt(Integer.MAX_VALUE))); 
		else if (PrimitiveUtils.isLong(clazz))
			return (F)new Long(Math.abs(random.nextLong()));
		else if (PrimitiveUtils.isShort(clazz))
			return (F)new Short((short)Math.abs(random.nextInt(Integer.MAX_VALUE >> 16)));
		else if (PrimitiveUtils.isDouble(clazz))
			return (F)new Double(Math.floor(Math.abs(random.nextDouble()))+.1);
		else if (PrimitiveUtils.isFloat(clazz))
			return (F)new Float(Math.floor(Math.abs(random.nextFloat()))+.1);		
		else if (PrimitiveUtils.isString(clazz))
			return (F)new String("String"+Math.abs(random.nextInt()));
		else if (PrimitiveUtils.isDate(clazz))
			return (F)calendar.getTime();
		else if (PrimitiveUtils.isBoolean(clazz))
			return (F)new Boolean(random.nextInt() % 2 == 0 ? true : false);
		else if (PrimitiveUtils.isObject(clazz))
			return (F)new Integer(-1);
		throw new RuntimeException(String.format("Class %s not supported", clazz.getSimpleName()));
	}
	static Calendar calendar;
	static {
		calendar = GregorianCalendar.getInstance();
		calendar.clear();
	}

}
