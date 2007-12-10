package org.hivedb.util.serialization;


/**
 *  Describes how to modernize the XML of a domain object from an old version to the current version.
 * @author andylikuski
 *
 * @param <T> the domain class
 */
public interface XmlModernizationPaver<T> {
	public Modernizer<T> getModernizer(Integer fromVersion, Integer toVersion);

	public Integer getCurrentXmlVersion();
}
