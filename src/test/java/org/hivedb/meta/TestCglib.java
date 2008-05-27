package org.hivedb.meta;

import java.util.Collection;

import org.hivedb.util.classgen.GeneratedClassFactory;
import org.hivedb.util.classgen.GeneratedInstanceInterceptor;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestCglib {
	
	@Test
	public void testCGLib() {
	    Foo  foo =  (Foo)GeneratedClassFactory.newInstance( Foo.class );

	    try {
			GeneratedInstanceInterceptor.setProperty(foo, "sampleProperty","TEST");
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	    String sampleProperty = foo.getSampleProperty();
	    Assert.assertEquals(sampleProperty,"TEST");
	}
	
	public interface Foo {
	     public String getSampleProperty();
	     public String boosToString(Collection<Boo> boos);
	} 
	public interface Boo {}
}
