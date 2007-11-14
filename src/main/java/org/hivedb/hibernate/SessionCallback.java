package org.hivedb.hibernate;

import org.hibernate.Session;

public interface SessionCallback {
	public void execute(Session session);
}
