package org.hivedb.hibernate;

import org.hibernate.shards.session.ShardedSessionFactory;

public interface HiveSessionFactoryBuilder {
	public ShardedSessionFactory getSessionFactory();
}
