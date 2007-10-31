package org.hivedb.util;

import java.util.Collection;

import org.hivedb.HiveKeyNotFoundException;
import org.hivedb.HiveReadOnlyException;
import org.hivedb.HiveRuntimeException;
import org.hivedb.Lockable;
import org.hivedb.meta.IdAndNameIdentifiable;
import org.hivedb.meta.Identifiable;

public class Preconditions {
	@SuppressWarnings("unchecked")
	public static<T extends IdAndNameIdentifiable> void nameIsUnique(Collection<T> collection, T item) {
		if(!IdentifiableUtils.isNameUnique((Collection<IdAndNameIdentifiable>) collection, item))
				throw new HiveRuntimeException(
						String.format("%s with name %s already exists", item.getClass().getSimpleName(), item.getName()));
	}
	
	@SuppressWarnings("unchecked")
	public static<T extends Identifiable> void idIsPresentInList(Collection<T> collection, T item) {
		if(!IdentifiableUtils.isIdPresent((Collection<IdAndNameIdentifiable>)collection, (IdAndNameIdentifiable) item))
			throw new HiveKeyNotFoundException(
					String.format("Could not find %s with id %s", item.getClass().getSimpleName(), item.getId()), item);
	}
	
	public static void isWritable(Collection<? extends Lockable> lockables, Lockable... moreLockables) throws HiveReadOnlyException {
		for(Lockable lockable : lockables)
			isWritable(lockable);
		isWritable(moreLockables);
	}
	
	public static void isWritable(Lockable... lockables) throws HiveReadOnlyException {
		for(Lockable lockable : lockables)
			isWritable(lockable);
	}
	
	public static void isWritable(Lockable lockable) throws HiveReadOnlyException {
		if(lockable.isReadOnly())
			throw new HiveReadOnlyException(
					String.format("This operation is invalid because the %s is currently read-only.", lockable.getClass().getSimpleName()));
	}
	
	public static void isNotEmpty(Collection c, String message) throws HiveKeyNotFoundException {
		if(c == null || c.size() == 0)
			throw new HiveKeyNotFoundException(message);
	}
}
