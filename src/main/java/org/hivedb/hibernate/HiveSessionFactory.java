package org.hivedb.hibernate;

import org.hibernate.Interceptor;
import org.hibernate.Session;

public interface HiveSessionFactory {
	public Session openSession();
	public Session openSession(Interceptor interceptor);
	public Session openSession(Object primaryIndexKey);
	public Session openSession(Object primaryIndexKey, Interceptor interceptor);
	public Session openSession(String resource, Object resourceId);
	public Session openSession(String resource, Object resourceId, Interceptor interceptor);
	public Session openSession(String resource, String indexName, Object secondaryIndexKey);
	public Session openSession(String resource, String indexName, Object secondaryIndexKey, Interceptor interceptor);
	/**
	 * Returns the default interceptor for use as a delegate to a custom interceptor
	 * @return
	 */
	public Interceptor getDefaultInterceptor();
}
