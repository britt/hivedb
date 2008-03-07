package org.hivedb.meta;

import org.hivedb.Lockable;
import org.hivedb.util.HiveUtils;

public class KeySemaphore implements Lockable {
	private Status status;
	private int id;
	
	public KeySemaphore(int id, Lockable.Status status) {
		this.id = id;
		this.status = status;
	}
	
	public int getId() {
		return id;
	}
		
	public boolean equals(Object obj)
	{
		return obj.hashCode() == hashCode();
	}
	public int hashCode() {
		return HiveUtils.makeHashCode(new Object[] {
				id, status
		});
	}

	public Status getStatus() {
		return status;
	}


}
