/**
 * 
 */
package org.hivedb.util.serialization;

import org.hivedb.util.AccessorFunction;


public class GetSetWrapperFunction<T> extends AccessorFunction<T> {
	Class<T> fieldClass;
	T fieldValue;
	public GetSetWrapperFunction(final Class<T> fieldClass, T fieldValue) {
		this.fieldClass = fieldClass;
		this.fieldValue = fieldValue;
	}
	public Class getFieldClass() { return fieldClass; }
	public T get() { return fieldValue; }				
	public void set(T value) { fieldValue = value; }
}