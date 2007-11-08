package org.hivedb.hibernate;

import java.io.Serializable;

import org.hibernate.shards.engine.ShardedSessionFactoryImplementor;

public interface HiveSessionFactoryBuilder {
	public ShardedSessionFactoryImplementor getSessionFactory();
}
