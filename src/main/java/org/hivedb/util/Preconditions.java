package org.hivedb.util;

import org.hivedb.HiveKeyNotFoundException;
import org.hivedb.HiveLockableException;
import org.hivedb.HiveRuntimeException;
import org.hivedb.Lockable;
import org.hivedb.meta.IdAndNameIdentifiable;
import org.hivedb.meta.Identifiable;

import java.util.Collection;

public class Preconditions {	
	@SuppressWarnings("unchecked")
	public static<T extends IdAndNameIdentifiable> boolean isNameUnique(Collection<T> collection, String name) {
		return IdentifiableUtils.isNameUnique((Collection<IdAndNameIdentifiable>) collection, name);
	}
	
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
	
	public static void isWritable(Collection<? extends Lockable> lockables, Lockable... moreLockables) throws HiveLockableException {
		for(Lockable lockable : lockables)
			isWritable(lockable);
		isWritable(moreLockables);
	}
	
	public static void isWritable(Lockable... lockables) throws HiveLockableException {
		for(Lockable lockable : lockables)
			isWritable(lockable);
	}
	
	public static void isWritable(Lockable lockable) throws HiveLockableException {
		if(lockable.getStatus() != Lockable.Status.writable)
			throw new HiveLockableException(
					String.format("This operation is invalid because the %s is currently set to status %s.", lockable.getClass().getSimpleName(), lockable.getStatus()));
		}
	
	public static void isNotEmpty(Collection c, String message) throws HiveKeyNotFoundException {
		if(c == null || c.size() == 0)
			throw new HiveKeyNotFoundException(message);
	}

  public static void isNotNull(Object... objects) {
    for(Object o : objects)
      if(o == null) {throw new HiveRuntimeException("Precondition violated an object was null.");}
  }
}
