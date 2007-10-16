package org.hivedb.util;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import org.hivedb.meta.Node;
import org.hivedb.meta.KeySemaphore;
import org.hivedb.meta.PartitionDimension;
import org.hivedb.util.functional.Transform;
import org.hivedb.util.functional.Unary;

public class HiveUtils {
	/**
	 *  This belongs in a utility class. Takes field values of an instance to generate a hash code
	 * @param objects
	 * @return
	 */
	public static int makeHashCode(Object[] objects) {
		return makeHashCode(Arrays.asList(objects));
	}
	public static int makeHashCode(Collection<?> collection) {
		String result = "";
		for (Object object : collection)
			if (object != null)
				result += (object instanceof Collection ? new HashSet<Object>((Collection<?>)object).hashCode() : object.hashCode());
		return result.hashCode();
	}
	static int globalDeepFormatedStringTabLevel = 0;
	public static String toDeepFormatedString(Object target, Object... alteringKeyValues) {
		
		String tabs = makeTabs(globalDeepFormatedStringTabLevel);
		String formatString = "\n"+tabs + "%s (HashCode:%s)\n";
		Object[] values = new Object[2+alteringKeyValues.length/2];
		values[0] = target.getClass().getSimpleName();
		values[1] = target.hashCode();
		tabs = makeTabs(++globalDeepFormatedStringTabLevel);
		for (int i=0; i<alteringKeyValues.length; i+=2)
		{
			formatString += "\n"+ tabs + alteringKeyValues[i] + ":%s";
			values[i/2+2] = alteringKeyValues[i+1] instanceof Collection
				?toDeepFormatedStringOfCollection(Arrays.asList(alteringKeyValues[i+1]))
				:alteringKeyValues[i+1];
		}
		globalDeepFormatedStringTabLevel--;
		return String.format(formatString, values);					
	}

	public static String toDeepFormatedStringOfCollection(Collection<Object> collection)
	{
		String tabs = makeTabs(++globalDeepFormatedStringTabLevel);
		Set<Object> set = new TreeSet<Object>(collection);
		
		Object[] objects = new Object[1+set.size()];
		String formatString = "[(Collection HashCode:%s)\n";
		objects[0] = set.hashCode();
		Iterator<?> iterator = set.iterator();
		for(int i=1; i<objects.length; i++) {
			objects[i] = iterator.next();
			formatString += tabs + "%s" + (iterator.hasNext() ? ", " : "");
		}
		formatString += "]";
		globalDeepFormatedStringTabLevel--;
		return String.format(formatString, objects);			
	}
	private static String makeTabs(int count)
	{
		String tabs = "";
		for (int i=0; i<count; i++) tabs += "\t";
		return tabs;
	}
	
	public static Collection<Node> getNodesForSemaphores(Collection<KeySemaphore> semaphores, final PartitionDimension dimension) {
		return Transform.map(new Unary<KeySemaphore, Node>(){
			public Node f(KeySemaphore item) {
				return dimension.getNode(item.getId());
			}}, semaphores);
	}
	
	public static<T> T defaultTo(T obj, T value) {
		return obj == null ? value : obj;
	}
}
