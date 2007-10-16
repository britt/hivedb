package org.hivedb.util;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Properties;

import org.apache.velocity.Template;
import org.apache.velocity.app.Velocity;
import org.apache.velocity.context.Context;
import org.apache.velocity.exception.MethodInvocationException;
import org.apache.velocity.exception.ParseErrorException;
import org.apache.velocity.exception.ResourceNotFoundException;
import org.hivedb.HiveRuntimeException;

public class Templater {
	public static String render(String templateFile, Context context) {
		try {
			Velocity.init(getDefaultVelocityProperties());
		} catch (Exception e) {
			throw new HiveRuntimeException("Failed to initialize Velocity templatng engine.");
		}
		
		Template template = null;
		try {
			template = Velocity.getTemplate(templateFile);
		} catch (ResourceNotFoundException e) {
			throw new HiveRuntimeException("Unable to locate template: " + templateFile);
		} catch (ParseErrorException e) {
			throw new HiveRuntimeException("Error parsing template: " + templateFile);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
		StringWriter writer = new StringWriter();
		try {
			template.merge(context, writer);
		} catch (ResourceNotFoundException e) {
			throw new HiveRuntimeException("Unable to locate template: " + templateFile);
		} catch (ParseErrorException e) {
			throw new HiveRuntimeException("Error parsing template: " + templateFile);
		} catch (MethodInvocationException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new HiveRuntimeException("Error reading template: " + templateFile);
		}
		
		return writer.toString();
	}
	
	public static Properties getDefaultVelocityProperties() {
		Properties p = new Properties();
		p.setProperty( "resource.loader", "class" );
		p.setProperty( "class.resource.loader.class", "org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader" );
		return p;
	}
}
