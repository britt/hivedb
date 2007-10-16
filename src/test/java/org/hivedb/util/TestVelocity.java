package org.hivedb.util;

import java.io.StringWriter;
import java.sql.Types;
import java.util.Properties;

import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.Velocity;
import org.hivedb.meta.PartitionDimension;
import org.hivedb.meta.persistence.IndexSchema;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.*;

public class TestVelocity {
	private static String expected = "Hello HiveDB World!";
	@Test
	public void testTemplating() throws Exception {
		Properties p = new Properties();
		p.setProperty( "resource.loader", "class" );
		p.setProperty( "class.resource.loader.class", "org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader" );
		Velocity.init(p);
		VelocityContext context = new VelocityContext();
		context.put("foo", "HiveDB");
		Template template = Velocity.getTemplate("test_template.vtl");
		StringWriter writer = new StringWriter();
		template.merge(context, writer);
		assertEquals(expected, writer.toString());
//		System.out.println(writer.toString());
	}
	
	@Test
	public void testTemplater() throws Exception {
		VelocityContext context = new VelocityContext();
		context.put("foo", "HiveDB");
		String output = Templater.render("test_template.vtl", context);
		assertEquals(expected, output);
	}
}
