package org.hivedb.meta;

import org.hivedb.util.HiveUtils;

public class NodeSemaphore {
	private boolean readOnly;
	private int id;
	
	public NodeSemaphore(int id, boolean readOnly) {
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
