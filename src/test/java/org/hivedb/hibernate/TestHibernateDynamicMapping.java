package org.hivedb.hibernate;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import org.hibernate.EntityMode;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.hivedb.hibernate.HiveSessionFactoryBuilderImpl;
import org.hivedb.meta.Node;
import org.hivedb.util.PropertyAccessor;
import org.hivedb.util.classgen.GenerateInstance;
import org.hivedb.util.classgen.GeneratedInstanceInterceptor;
import org.hivedb.util.database.test.H2HiveTestCase;
import org.hivedb.util.database.test.HiveTest;
import org.hivedb.util.functional.Atom;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

public class TestHibernateDynamicMapping extends HiveTest {
//	
//	@Test
//	public void testMapping() throws Exception {
//		WeatherReport instance = new GenerateInstance<WeatherReport>(WeatherReport.class).generate();
//		AssertJUnit.assertNotNull(instance);
//		Map<String,Object> m =  
//			((PropertySetter<WeatherReport>)instance).getAsMap();
//	    
//		AssertJUnit.assertNotNull(m);
//		
//		Node node = Atom.getFirst(getHive().getNodes());
//		Configuration c = HiveSessionFactoryBuilderImpl.createConfigurationFromNode(node, new Properties());
//		
//		SessionFactory f = c.addClass(WeatherReport.class).configure().buildSessionFactory();
//		Session session = f.openSession().getSession(EntityMode.MAP);
//		session.save(WeatherReport.class.getName(), m);
//		
//		Map persistent = (Map) session.get(WeatherReport.class, instance.getReportId());
//		AssertJUnit.assertNotNull(persistent);
//		WeatherReport r = GeneratedInstanceInterceptor.newInstance(WeatherReport.class, persistent);
//		AssertJUnit.assertEquals(instance, r);
//	}
//
//	@Override
//	protected Collection<String> getDataNodeNames() {
//		return Collections.singletonList("data_node");
//	}
//	
}
