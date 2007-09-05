package org.hivedb.meta;

import org.hivedb.Lockable;
import org.hivedb.util.HiveUtils;

public class KeySemaphore implements Lockable {
	private boolean readOnly;
	private int id;
	
	public KeySemaphore(int id, boolean readOnly) {
		this.id = id;
		this.readOnly = readOnly;
	}
	
	public int getId() {
		return id;
	}
	
	public boolean isReadOnly() {
		return readOnly;
	}
	
	public boolean equals(Object obj)
	{
		return obj.hashCode() == hashCode();
	}
	public int hashCode() {
		return HiveUtils.makeHashCode(new Object[] {
				id, readOnly
		});
	}
}
