package org.hivedb.util.serialization;

import java.io.InputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

import org.hivedb.util.PrimitiveUtils;
import org.hivedb.util.ReflectionTools;
import org.hivedb.util.functional.Filter;
import org.hivedb.util.functional.Predicate;
import org.hivedb.util.functional.Transform;
import org.hivedb.util.functional.Unary;
import org.hivedb.util.functional.Transform.IdentityFunction;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.converters.Converter;
import com.thoughtworks.xstream.converters.MarshallingContext;
import com.thoughtworks.xstream.converters.SingleValueConverterWrapper;
import com.thoughtworks.xstream.converters.UnmarshallingContext;
import com.thoughtworks.xstream.io.HierarchicalStreamReader;
import com.thoughtworks.xstream.io.HierarchicalStreamWriter;


public class XmlXStreamSerializer<RAW> implements Serializer<RAW, InputStream> {
	Class<RAW> representedInterface;
	
	XStream xStream;
	Map<Class<?>, ClassXmlTransformer> classXmlTransformerMap;
	@SuppressWarnings("unchecked")
	public XmlXStreamSerializer(final Class<?> representedInterface)
	{
		this.representedInterface = (Class<RAW>) representedInterface;
		Collection<Class<?>> propertyTypes = ReflectionTools.getUniqueComplexPropertyTypes(Collections.singletonList(representedInterface));
		classXmlTransformerMap = Transform.toMap(
				new IdentityFunction<Class<?>>(),
				new Unary<Class<?>, ClassXmlTransformer>() {
					public ClassXmlTransformer f(Class<?> propertyType ) {
						return new ClassXmlTransformerImpl(representedInterface);
				}},
				propertyTypes);
		
		this.xStream = new XStream();
		for (ClassXmlTransformer<Object> classXmlTransformer : classXmlTransformerMap.values()) {
			xStream.registerConverter(new ClassConverter(classXmlTransformer));
			Object instancePrototype = classXmlTransformer.createInstance();
			xStream.alias(classXmlTransformer.abbreviate(classXmlTransformer.getRespresentedInterface().getSimpleName().toLowerCase()), instancePrototype.getClass());
		}
	}
	
	@SuppressWarnings("unchecked")
	public InputStream serialize(final RAW raw) {	
		RAW objectToSerialize = (RAW) resolveClassXmlTransformer(raw).wrapInSerializingImplementation(raw);
		return Compression.compress(xStream.toXML(objectToSerialize));
	}
	
	private ClassXmlTransformer resolveClassXmlTransformer(final Object instance) {
		return classXmlTransformerMap.get(
			Filter.grepSingle(new Predicate<Class<?>>() {
				public boolean f(Class<?> classXmlClass) {
					return ReflectionTools.doesImplementOrExtend(instance.getClass(), classXmlClass);
				}},
				classXmlTransformerMap.keySet()));
	}
	@SuppressWarnings("unchecked")
	public RAW deserialize(InputStream serial) {
		return (RAW)xStream.fromXML(Compression.decompress(serial));
	}
	
	public class ClassConverter implements Converter {    

		private ClassXmlTransformer<Object> classXmlTransformer;
		public ClassConverter(ClassXmlTransformer<Object> classXmlTransformer) {
			this.classXmlTransformer = classXmlTransformer;
		}
		
		@SuppressWarnings("unchecked")
		public void marshal(Object source, HierarchicalStreamWriter writer, MarshallingContext context) {	
			marshalAttributes(source, writer);
	       	marshalNodes(source, writer, context);			
	    }

		@SuppressWarnings("unchecked")
		private void marshalAttributes(Object source, HierarchicalStreamWriter writer) {
	       	final Class<Object> respresentedInterface = classXmlTransformer.getRespresentedInterface();
			for (String propertyName : ReflectionTools.getPropertiesOfPrimitiveGetters(respresentedInterface))
	       	{		
	       		Object value = ReflectionTools.invokeGetter(source, propertyName);
	       		Class<?> fieldClass = ReflectionTools.getPropertyType(respresentedInterface, propertyName);
	       		if (value != null)
	       			try {
	       				writer.addAttribute(
	       					classXmlTransformer.abbreviate(propertyName),
	       					((SingleValueConverterWrapper)xStream.getConverterLookup().lookupConverterForType(fieldClass)).toString(value));
	       			}
	       			catch (Exception e) {
	       				throw new RuntimeException(e);
	       			}
	       	}
		}
		
		@SuppressWarnings("unchecked")
		private void marshalNodes(Object source, HierarchicalStreamWriter writer, MarshallingContext context) {
		 	final Class<Object> respresentedInterface = classXmlTransformer.getRespresentedInterface();
		 	for (String propertyName : ReflectionTools.getPropertiesOfComplexGetters(respresentedInterface))
	       	{		
	       		Object value = ReflectionTools.invokeGetter(source, propertyName);
	       		if (new NullOrEmptyRejector().filter(value)) {
	       			writer.startNode(classXmlTransformer.abbreviate(propertyName));		       		
					context.convertAnother(value);
		       		writer.endNode();
	       		}
	       	}
		}
								
	    public Object unmarshal(HierarchicalStreamReader reader, UnmarshallingContext context) {
	    	Object instance = classXmlTransformer.createInstance();
	    	
	    	Integer xmlVersion = seekXmlVersion(reader, context, classXmlTransformer);
			UnmarshallInterceptor<Object> unmarshallInterceptor = new UnmarshalModernizer<Object>(
	    			classXmlTransformer.getModernizer(xmlVersion, classXmlTransformer.getCurrentXmlVersion()));
	    	
	    	unmarshalAttributes(reader, instance, unmarshallInterceptor);		 
	    	unmarshalNodes(reader, context, instance, unmarshallInterceptor);	
	    	
			return unmarshallInterceptor.postUnmarshalInstanceModifier(instance);	    	
	    }

	    @SuppressWarnings("unchecked")
		private void unmarshalAttributes(HierarchicalStreamReader reader, Object instance, UnmarshallInterceptor<Object> unmarshalInterceptor) {
			Iterator iterator = reader.getAttributeNames();
			
			final Class<Object> respresentedInterface = classXmlTransformer.getRespresentedInterface();
			Map<String,String> abbreviatedNameToPropertyName = Transform.toMap(
				new Unary<String,String>() {
					public String f(String propertyName) {
						return classXmlTransformer.abbreviate(propertyName);
					}},
				new IdentityFunction<String>(),
				ReflectionTools.getPropertiesOfPrimitiveGetters(respresentedInterface));
			
			while (iterator.hasNext())
	    	{
	    		String abreviatedAttributeName = (String)iterator.next();
	    		String updatedAbreviatedAttributeName = unmarshalInterceptor.preUnmarshalAbreviatedElementNameTransformer(abreviatedAttributeName);
				if (!abbreviatedNameToPropertyName.containsKey(updatedAbreviatedAttributeName))
					throw new RuntimeException(String.format("The abreviated attribute name %s is not recognized by the ClassXmlTransformer for %s", updatedAbreviatedAttributeName, respresentedInterface.getName()));
	    		String fullAttributeName = abbreviatedNameToPropertyName.get(updatedAbreviatedAttributeName);
	    		if (unmarshalInterceptor.isElementedDeleted(fullAttributeName))
	    			continue;
	    		// propertyName will match fullAttributeName unless the interceptor changes it
				String propertyName = unmarshalInterceptor.preUnmarshalElementNameTransformer(
					fullAttributeName);
	    		
	    		// Assume attributes are simple converters
	    		SingleValueConverterWrapper converter = getAttributeConverter(
	    				ReflectionTools.getPropertyType(respresentedInterface, propertyName));
	    		try {
	    			ReflectionTools.invokeSetter(
	    				instance, 
	    				propertyName,
	    				unmarshalInterceptor.postUnmarshalElementValueTransformer(
	    					propertyName,
	    					converter.fromString(
	    							reader.getAttribute(abreviatedAttributeName))));
	    		}
	    		catch (Exception e) {
	    			throw new RuntimeException(String.format("Error unmarshalling attribute %s of class %s", abreviatedAttributeName, instance.getClass().getSimpleName()), e);
	    		}
	    	}
		}
	    
		@SuppressWarnings("unchecked")
		private void unmarshalNodes(HierarchicalStreamReader reader, UnmarshallingContext context, Object instance, UnmarshallInterceptor<Object> unmarshalInterceptor) {
			final Class<?> respresentedInterface = classXmlTransformer.getRespresentedInterface();
			Map<String,String> abbreviatedNameToPropertyName = Transform.toMap(
				new Unary<String,String>() {
					public String f(String propertyName) {
						return classXmlTransformer.abbreviate(propertyName);
					}},
				new IdentityFunction<String>(),
				ReflectionTools.getPropertiesOfComplexGetters(respresentedInterface));
			
			while (reader.hasMoreChildren())
	    	{
				reader.moveDown();
				String abreviatedNodeName = reader.getNodeName();
				String updatedAbreviatedNodeName = unmarshalInterceptor.preUnmarshalAbreviatedElementNameTransformer(abreviatedNodeName);
				if (!abbreviatedNameToPropertyName.containsKey(updatedAbreviatedNodeName))
					throw new RuntimeException(String.format("The abreviated node name %s is not recognized by the ClassXmlTransformer of %s", updatedAbreviatedNodeName, classXmlTransformer.getRespresentedInterface().getName()));
	    		
				String fullNodeName = abbreviatedNameToPropertyName.get(updatedAbreviatedNodeName);
				if (unmarshalInterceptor.isElementedDeleted(fullNodeName))
	    			continue;
				// propertyName will match fullNodeName unless the interceptor changes it
				String propertyName = unmarshalInterceptor.preUnmarshalElementNameTransformer(fullNodeName);
				
		    	Class fieldClass = classXmlTransformer.wrapInSerializingImplementation(instance).getClass();
		    
		    	ReflectionTools.invokeSetter(
		    			instance,
		    			propertyName,
		    			unmarshalInterceptor.postUnmarshalElementValueTransformer(
		    				propertyName,
		    				context.convertAnother(instance,fieldClass)));
		    	reader.moveUp();
	    	}
		}

	    private Integer seekXmlVersion(HierarchicalStreamReader reader, UnmarshallingContext context, ClassXmlTransformer<Object> classXmlTransformer) {
			String BLOB_VERSION_ATTRIBUTE_ABREVIATION = "bv";
			
	    	String version = reader.getAttribute(BLOB_VERSION_ATTRIBUTE_ABREVIATION);
	    	if (version != null)
	    		context.put(BLOB_VERSION_ATTRIBUTE_ABREVIATION, version);
	    	else
	    		version = (String) context.get(BLOB_VERSION_ATTRIBUTE_ABREVIATION);
	    	if (version == null)
	    		throw new RuntimeException("No version found in XML or MarshallingContext");
	    	SingleValueConverterWrapper converter = getAttributeConverter(Integer.class);
	    	return (Integer) converter.fromString(version);
		}
	    
		private SingleValueConverterWrapper getAttributeConverter(Class fieldClass) {
			return (SingleValueConverterWrapper)xStream.getConverterLookup().lookupConverterForType(fieldClass);
		}
	    
		public boolean canConvert(Class type) {
	    	return PrimitiveUtils.isPrimitiveClass(type)
	    	  && ReflectionTools.doesImplementOrExtend(
    				type,
    				this.classXmlTransformer.getRespresentedInterface());
	    }
	   
	}
	
	
	
	// This interface maps one-to-one to Modernizer, but is named with more general methods
	// for other possible uses.
	protected interface UnmarshallInterceptor<T>
	{
		String preUnmarshalAbreviatedElementNameTransformer(String abreviatedElementName);
		String preUnmarshalElementNameTransformer(String elementName);
		Boolean isElementedDeleted(String elementName);
		Object postUnmarshalElementValueTransformer(String elementName, Object elementValue);
		T postUnmarshalInstanceModifier(T instance);
	}
	
	protected static class UnmarshalModernizer<T> implements UnmarshallInterceptor<T> {
		Modernizer<T> modernizer;
		public UnmarshalModernizer(Modernizer<T> modernizer)
		{
			this.modernizer = modernizer;
		}
		
		public String preUnmarshalAbreviatedElementNameTransformer(String abreviatedElementName) {
			return modernizer.getNewAbreviatedElementName(abreviatedElementName);
		}
		
		/**
	     *  Preprocesses an XML element name, either an attribute or node name, and possibly modifies the name
	     * @param elementName the name of the XML attribute or node
	     * @return The optionally modified name
	     */
	    public String preUnmarshalElementNameTransformer(String elementName) {
	    	return modernizer.getNewElementName(elementName);
		}
	    
	    /**
	     * Postproccess an object deserialized from XML, either an attribute value or a node. 
	     * @param object
	     * @return
	     */
	    public Object postUnmarshalElementValueTransformer(String elementName, Object elementValue) {
	    	return modernizer.getUpdatedElementValue(elementName, elementValue);
		}

	    /**
	     * Reports whether or not an element represented by the given name, either of an attribute or node,
	     * has been deleted and is no longer represented in the instance.
	     */
		public Boolean isElementedDeleted(String elementName) {
			return modernizer.isDeletedElement(elementName);
		}

		public T postUnmarshalInstanceModifier(T instance) {
			return modernizer.modifyInstance(instance);
		}

	
	}
	public static class NullOrEmptyRejector
	{
		public boolean filter(Object value) {
			return value != null 
				&& !(ReflectionTools.doesImplementOrExtend(value.getClass(), Collection.class) && ((Collection)value).size()==0);
		}		
	}
	public Class<RAW> getRepresentedInterface() {
		return representedInterface;
	}
}
