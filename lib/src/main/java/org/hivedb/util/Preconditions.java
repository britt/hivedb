package org.hivedb.util;

import java.util.Collection;

import org.hivedb.HiveReadOnlyException;
import org.hivedb.HiveRuntimeException;
import org.hivedb.Lockable;
import org.hivedb.meta.IdAndNameIdentifiable;

public class Preconditions {
	

	@SuppressWarnings("unchecked")
	public static<T extends IdAndNameIdentifiable> void nameIsUnique(Collection<T> collection, T item) {
		if(!IdentifiableUtils.isNameUnique((Collection<IdAndNameIdentifiable>) collection, item))
				throw new HiveRuntimeException(
						String.format("%s with name %s already exists", item.getClass().getSimpleName(), item.getName()));
	}
	
	public static void isWritable(Collection<? extends Lockable> lockables) throws HiveReadOnlyException {
		for(Lockable lockable : lockables)
			isWritable(lockable);
	}
	
	public static void isWritable(Lockable... lockables) throws HiveReadOnlyException {
		for(Lockable lockable : lockables)
			isWritable(lockable);
	}
	
	public static void isWritable(Lockable lockable) throws HiveReadOnlyException {
		if (lockable.isReadOnly())
			throw new HiveReadOnlyException(
					String.format("This operation is invalid because the %s is currently read-only.", lockable.getClass().getSimpleName()));
	}
}
