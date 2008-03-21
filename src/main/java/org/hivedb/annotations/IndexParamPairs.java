package org.hivedb.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Map;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface IndexParamPairs {
	
	/**
	 *  Each pair of values is
	 *  i) a method parameter index value of the property name of an EntityConfig
	 *  i+1) a method parameter index value of the sought value of the EntityConfig
	 * @return
	 */
	int[] value();
}