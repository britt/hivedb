package org.hivedb.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 *  Represents a delegate property name to which to delegate data index queries. Currently the
 *  property must be in the same class as the annotated method.
 * @author andylikuski
 *
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface DataIndexDelegate {
	String value();
}

