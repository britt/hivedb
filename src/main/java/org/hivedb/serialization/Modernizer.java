package org.hivedb.serialization;


/**
 *  Describes how to modernize an old version of a domain class instance's XML to the modern version of the
 *  domain class. It allows for XML element name changes and deserialized value changes.
 *  This interface works in loose conjunction with the data structures returned by ClassXMLTransformer's methods.
 * @author andylikuski
 *
 * @param <T> The domain class that is being deserialized
 */
public interface Modernizer<T> {
	/**
	 *  Modifies any attribute or node name abreviation that should be changed to a new name. Changing
	 *  the root node name is not necessarily supported.
	 * @param abreviatedElementName
	 * @return
	 */
	String getNewAbreviatedElementName(String abreviatedElementName);
	/**
	 *  Modifies any attribute or node full name that should be changed to a new name to correspond
	 *  to a change in field name in the domain object class.
	 * @param elementName
	 * @return
	 */
	String getNewElementName(String elementName);
	/**
	 *  Returns true if the given attribute or node has been deleted from the new version of the XML.
	 *  This will normally occur if the corresponding domain object class field has been deleted, or
	 *  can be used to set the field back to its default value. In either case returning true should
	 *  indicated to the deserializer to not set the corresponding field value of the instance being
	 *  deserialized.
	 * @param elementName The name corresponding to the field of the domain class (not the abreviated
	 * XML name.)
	 * @return
	 */
	Boolean isDeletedElement(String elementName);
	/**
	 *  Modifies the value of a deserialized attribute or node.
	 * @param elementName The name corresponding to the field of the domain class (not the abreviated
	 * XML name.)
	 * @param elementValue The value that was deserialized from the XML
	 * @return The new value to be assigned to the deserialized instance's field.
	 */
	Object getUpdatedElementValue(String elementName, Object elementValue);
	/**
	 *  Mod
	 * @param instance
	 * @return
	 */
	T modifyInstance(T instance);

}
