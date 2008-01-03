package org.hivedb.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import org.springframework.beans.factory.BeanDefinitionStoreException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.test.context.ContextLoader;

public class ClassNameContextLoader implements ContextLoader {
	public ApplicationContext loadContext(String... locations) throws Exception {
		return new ClassPathXmlApplicationContext(locations);
	}

	public String[] processLocations(Class<?> clazz, String... locations) {
		Collection<String> xmls = new ArrayList<String>();
		if(locations != null && locations.length != 0)
			xmls.addAll(Arrays.asList(locations));
		try {
			if(new DefaultResourceLoader().getResource(clazz.getName() + ".xml").exists())
				xmls.add(clazz.getName() + ".xml");
		} catch(BeanDefinitionStoreException e) {
			//quash when the file does not exist
		}
		return xmls.toArray(new String[]{});
	}
}