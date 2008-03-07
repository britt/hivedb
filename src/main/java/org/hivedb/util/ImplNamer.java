/**
 * 
 */
package org.hivedb.util;

import net.sf.cglib.core.DefaultNamingPolicy;
import net.sf.cglib.core.Predicate;

import org.hivedb.annotations.GeneratedClass;

class ImplNamer extends DefaultNamingPolicy {
	private Class representedInterface;
	public ImplNamer(Class representedInterface) {
		this.representedInterface = representedInterface;
	}
	public String getClassName(String prefix, String source, Object key, Predicate names) {
		return representedInterface.getAnnotation(GeneratedClass.class) != null
					? removeClass(representedInterface.getCanonicalName()) + ((GeneratedClass)  representedInterface.getAnnotation(GeneratedClass.class)).value()
					: super.getClassName(prefix, source, key, names);

	}
	private String removeClass(String prefix) {
		return prefix.substring(0,prefix.lastIndexOf(".")+1); // maintains the final dot
	}
}