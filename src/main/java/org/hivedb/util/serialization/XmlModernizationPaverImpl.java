package org.hivedb.util.serialization;

import java.util.Collection;
import java.util.Hashtable;
import java.util.Map;

import org.hivedb.util.functional.Amass;
import org.hivedb.util.functional.Filter;
import org.hivedb.util.functional.Joiner;
import org.hivedb.util.functional.NumberIterator;
import org.hivedb.util.functional.Predicate;
import org.hivedb.util.functional.Transform;

public class XmlModernizationPaverImpl<T>  implements XmlModernizationPaver {
	private ClassXmlTransformer<T> classXmlTransformer;
	private Integer currentXmlVersion;
	private Map<Integer,Modernizer<T>> modernizerMap;
	public XmlModernizationPaverImpl(
			ClassXmlTransformer<T> classXmlTransformer, 
			Integer currentXmlVersion,
			Map<Integer,Modernizer<T>> modernizerMap)
	{
		this.classXmlTransformer = classXmlTransformer;
		this.currentXmlVersion = currentXmlVersion;
		this.modernizerMap = modernizerMap;
	}
	
	public XmlModernizationPaverImpl(
			ClassXmlTransformer<T> classXmlTransformer, 
			Integer currentXmlVersion)
	{
		this(classXmlTransformer, currentXmlVersion, new Hashtable<Integer,Modernizer<T>>());
	}
	
	protected ClassXmlTransformer<T> getClassXmlTransformer() {
		return classXmlTransformer;
	}
	
	public Modernizer<T> getModernizer(Integer fromVersion, Integer toVersion) {
		return chainModernizers(fromVersion,toVersion);
	}
	
	public Integer getCurrentXmlVerson() {
		return currentXmlVersion;
	}
	
	private Modernizer<T> chainModernizers(Integer fromVersion, Integer toVersion) {
		
		// Collect all modernizers in oldest to newest
		Collection<Modernizer<T>> modernizers = Transform.map(new Transform.MapKeyToValueFunction<Integer, Modernizer<T>>(modernizerMap),
							Filter.grep(new Predicate<Integer>() {
								public boolean f(Integer version) {
									return modernizerMap.containsKey(version);
								}	
							}, new NumberIterator(fromVersion, toVersion)));
		// If none exist return an IdentityModernizer
		if (modernizers.size() == 0)
			return new IdentityModernizer<T>();
		
		// Chain the modernizers together from oldest to newest and return the resulting modernizer
		return (Modernizer<T>) Amass.join(new Joiner<Modernizer<T>, Modernizer<T>>() {
			public Modernizer<T> f(final Modernizer<T> toModernizer, final Modernizer<T> fromModernizer) {
				return new Modernizer<T>() {
					public String getNewAbreviatedElementName(String abreviatedElementName) {
						return toModernizer.getNewAbreviatedElementName(
									fromModernizer.getNewAbreviatedElementName(abreviatedElementName));
					}
					public String getNewElementName(String elementName) {
						return toModernizer.getNewElementName(
									fromModernizer.getNewElementName(elementName));
					}
					public Boolean isDeletedElement(String elementName) {
						return fromModernizer.isDeletedElement(elementName) ||toModernizer.isDeletedElement(elementName);
					}
					public Object getUpdatedElementValue(String elementName, Object elementValue) {
						return toModernizer.getUpdatedElementValue(elementName,
								fromModernizer.getUpdatedElementValue(elementName,elementValue));
					}
					public T modifyInstance(T instance) {
						return toModernizer.modifyInstance(
								fromModernizer.modifyInstance(instance));
					}
					
				};
			}},
			modernizers
		);
		
	}

	public Integer getCurrentXmlVersion() {
		return currentXmlVersion;
	}
}
