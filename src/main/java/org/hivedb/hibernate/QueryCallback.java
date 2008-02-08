package org.hivedb.hibernate;

import java.util.Collection;

import org.hibernate.Session;

public interface QueryCallback {
	public Collection<Object> execute(Session session);
}
