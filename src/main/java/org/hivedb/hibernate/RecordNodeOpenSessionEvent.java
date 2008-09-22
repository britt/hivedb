package org.hivedb.hibernate;

import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.shards.session.OpenSessionEvent;

import java.sql.SQLException;

public class RecordNodeOpenSessionEvent implements OpenSessionEvent {

	public static ThreadLocal<String> node = new ThreadLocal<String>();
	
	public static String getNode() {
		return node.get();
	}
	
	public static void setNode(Session session) {
		node.set(getNode(session));
	}
	
	public void onOpenSession(Session session) {
		setNode(session);
	}
	
	@SuppressWarnings("deprecation")
	private static String getNode(Session session) {
		String node = "";
		if (session != null) {
			try {
				node = session.connection().getMetaData().getURL();
			} catch (SQLException ex) {
			} catch (HibernateException ex) {
			}
		}
		return node;
	}
}
