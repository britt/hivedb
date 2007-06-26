/**
 * HiveDB is an Open Source (LGPL) system for creating large, high-transaction-volume
 * data storage systems.
 */
package org.hivedb.meta;

import org.hivedb.HiveRuntimeException;
import org.hivedb.util.HiveUtils;

/**
 * 
 * @author Andy Likuski (alikuski@cafepress.com)
 *
 */
public class Access {
	private AccessType accessType;
	private int readShareLevel;
	private int writeShareLevel;
	/**
	 * 
	 * @param accessType
	 * @param readShareLevel
	 * @param writeShareLevel
	 */
	public Access(AccessType accessType, int readShareLevel, int writeShareLevel) {
		super();
		this.accessType = accessType;
		this.readShareLevel = readShareLevel;
		this.writeShareLevel = writeShareLevel;
	}
	public AccessType getAccessType() {
		return accessType;
	}
	public int getReadShareLevel() {
		return readShareLevel;
	}
	public int getWriteShareLevel() {
		return writeShareLevel;
	}
	
	public static AccessType parseType(String accessTypeString) {
		if (AccessType.ReadWrite.toString().equals(accessTypeString))
			return AccessType.ReadWrite;
		if (AccessType.Read.toString().equals(accessTypeString))
			return AccessType.Read;
		throw new HiveRuntimeException("Unknown access type: " + accessTypeString);
	}
	public int hashCode() {
		return HiveUtils.makeHashCode(new Object[] {
				accessType, readShareLevel, writeShareLevel
		});
	}
	public String toString()
	{
		return HiveUtils.toDeepFormatedString(this, 
										"AccessType", 		getAccessType(), 
										"ReadShareLevel", 	getReadShareLevel(), 
										"WriteShareLevel", 	getWriteShareLevel());
	}
}
