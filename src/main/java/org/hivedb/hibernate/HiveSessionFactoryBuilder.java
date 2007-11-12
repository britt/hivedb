package org.hivedb.hibernate;

import org.hibernate.shards.engine.ShardedSessionFactoryImplementor;

public interface HiveSessionFactoryBuilder {
	public ShardedSessionFactoryImplementor getSessionFactory();
}
