/**
 * 
 */
package org.hivedb.util.database.test;

import java.util.Collection;
import java.util.Collections;

import org.hivedb.Schema;
import org.hivedb.hibernate.BaseDataAccessObject;
import org.hivedb.hibernate.ConfigurationReader;
import org.hivedb.services.BaseClassDaoService;
import org.hivedb.services.ClassDaoService;
import org.hivedb.util.functional.Delay;

public class LazyInitializer implements Delay<Object> {
	private Delay<?> delay;
	private Collection<Schema> schemaList;
	private SchemaInitializer test;
	public LazyInitializer(Delay<?> delay, Schema schema) {
		this.delay = delay;
		this.schemaList = Collections.singletonList(schema);
	}
	public LazyInitializer(Delay<?> delay, Collection<Schema> schemaList) {
		this.delay = delay;
		this.schemaList = schemaList;
	}
	public LazyInitializer(final Class clazz, Collection<Schema> schemaList, Delay delay){
		this.schemaList = schemaList;
		this.delay = delay; 
	}
	public LazyInitializer(final Class clazz, Schema schema, Delay delay){
		new LazyInitializer(clazz, Collections.singletonList(schema), delay);
	}
	public Object f() {
		test.initialize(schemaList);
		return delay.f();
	}
	public LazyInitializer setTest(SchemaInitializer test) {this.test = test; return this;}
}