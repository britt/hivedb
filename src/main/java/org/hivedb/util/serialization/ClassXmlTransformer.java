package org.hivedb.util.serialization;

import java.util.Collection;

import org.hivedb.util.PropertiesAccessor;

// Describes how to translate between a class and XML, namely what should be attributes, what should be XML,
// and encapsulates a call to a instance's getter and setter for the relative field
public interface ClassXmlTransformer<T> {
	/**
	 *  Returns the current version of the XML blob for this class. XML retrieved of o
	 * @return
	 */
	public Integer getCurrentXmlVersion();

	public PropertiesAccessor getPropertiesAccessor(final Object instance);
	
	String abbreviate(String name);
	public String elongate(String abbreviatedName);
	
	/**
	 *  The interface transformed by the transformer.
	 * @return
	 */
	public Class<T> getRespresentedInterface();
	/**
	 *  Returns a new instance of the transformed class to be populated during deserialization
	 * @return
	 */
	public T createInstance();
	/**
	 *  Optionally wraps an instance of the transformed interface in the actual implementation used for serialization. 
	 * @param instance
	 * @return
	 */
	public T wrapInSerializingImplementation(T instance);
	/**
	 *  Return a collection of the required transformer instnaces, including an instance of this
	 *  implementing class. If this class has no dependencies, the list will only have one element--
	 *  and instance of this class.
	 * @return
	 */
	public Collection<ClassXmlTransformer> getRequiredTransformers();
	/**
	 *  Returns an object that knows how to modernize any version of a serialized object to the modern version
	 * @return
	 */
	public Modernizer<T> getModernizer(Integer fromVersion, Integer toVersion);

}
