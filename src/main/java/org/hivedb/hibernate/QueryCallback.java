package org.hivedb.hibernate;

import org.hibernate.Session;

import java.util.Collection;

public interface QueryCallback {
	public Collection<Object> execute(Session session);
}
