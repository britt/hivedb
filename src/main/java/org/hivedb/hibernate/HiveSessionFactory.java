package org.hivedb.hibernate;

import java.util.List;

import org.hibernate.Session;
import org.hibernate.shards.util.InterceptorDecorator;

public interface HiveSessionFactory {
	public Session openSession();
	public Session openSession(InterceptorDecorator interceptor);
	public Session openSession(Object primaryIndexKey);
	public Session openSession(Object primaryIndexKey, InterceptorDecorator interceptor);
	public Session openSession(String resource, Object resourceId);
	public Session openSession(String resource, Object resourceId, InterceptorDecorator interceptor);
	public Session openSession(String resource, String indexName, Object secondaryIndexKey);
	public Session openSession(String resource, String indexName, Object secondaryIndexKey, InterceptorDecorator interceptor);
	public void appendInterceptor(InterceptorDecorator interceptor);
	public void prependInterceptor(InterceptorDecorator interceptor);
	public void insertInterceptor(InterceptorDecorator interceptor, int index);
	public List<InterceptorDecorator> getInterceptors();
}
