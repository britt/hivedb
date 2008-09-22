package org.hivedb.hibernate.simplified.session;

import org.hibernate.Session;
import org.hibernate.Interceptor;

public interface HiveSessionFactory {
  public Session openSession();
	public Session openSession(Interceptor interceptor);
	public Session openSession(Object primaryIndexKey);
	public Session openSession(Object primaryIndexKey, Interceptor interceptor);
	public Session openSession(String resource, Object resourceId);
	public Session openSession(String resource, Object resourceId, Interceptor interceptor);
	public Session openSession(String resource, String indexName, Object secondaryIndexKey);
	public Session openSession(String resource, String indexName, Object secondaryIndexKey, Interceptor interceptor);
}
