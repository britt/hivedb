package org.hivedb.test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.hibernate.shards.util.Lists;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

public class ContextAwareTest extends AbstractTestNGSpringContextTests{
	protected List<TestContextInitializer> testContexts = Lists.newArrayList();
	protected int defaultAutowireMode = AutowireCapableBeanFactory.AUTOWIRE_BY_NAME;

	@SuppressWarnings("unchecked")
	@BeforeClass
	public void getInitializers() {
		testContexts.addAll(((Map) applicationContext.getBeansOfType(TestContextInitializer.class)).values());
		applicationContext.getAutowireCapableBeanFactory().autowireBeanProperties(this, defaultAutowireMode, false);
	}
	
	@BeforeMethod
	public void before() {
		for(TestContextInitializer t : testContexts)
			t.beforeTest();
	}
	
	@AfterMethod
	public void after() {
		Collections.reverse(testContexts);
		for(TestContextInitializer t : testContexts)
			t.afterTest();
	}
	

	public void setContexts(List<TestContextInitializer> contexts) {
		this.testContexts = contexts;
	}

	
	public int getDefaultAutowireMode() {
		return defaultAutowireMode;
	}

	public void setDefaultAutowireMode(int defaultAutowireMode) {
		this.defaultAutowireMode = defaultAutowireMode;
	}
}
