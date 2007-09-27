package org.hivedb.util;

import org.hivedb.util.functional.Amass;

/**
 *  Adds a name to an AccessFunction so that it can be hashed uniquely
 * @author andylikuski
 *
 */
public abstract class NamedAccessorFunction<T> extends AccessorFunction<T>  {
	private String name;
	public NamedAccessorFunction(String name) {
		this.name = name;
	}
	@Override
	public int hashCode() {
		return Amass.makeHashCode(new Object[] { name.hashCode(), getFieldClass().hashCode() });
	}
}
