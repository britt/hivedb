package org.hivedb.util;

import java.beans.PropertyChangeListener;

public interface PropertyChangeListenerRegistrar {
	void addPropertyChangeListener(PropertyChangeListener listener);
	void removePropertyChangeListener(PropertyChangeListener listener);
}
