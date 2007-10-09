package org.hivedb.util;

import org.hivedb.util.functional.Amass;
import org.hivedb.util.functional.GetFunction;
import org.hivedb.util.functional.SetFunction;


public abstract class AccessorFunction<F> implements GetFunction<F>, SetFunction<F> {
	
	public int hashCode() {
		return Amass.makeHashCode(new Object[] {  get().hashCode(), getFieldClass().hashCode() });
	}
	
	public boolean equals(Object obj) {
		return obj.hashCode() == this.hashCode();
	}
}
