package org.hivedb.meta;



/**
 * 
 *  An interfaces whose implementations attempt to compensate for Java's denial of classes as first-class objects. 
 *  In a prototype-based language, as are most scripting languages, one can blur the line between class and instance
 *  by subclassing instances and adding new methods dyanically. Prototype fakes instance-subclassing with the
 *  instantiateFrom method, which receives an instance and uses it as the Prototype of a new instance
 * 
 *  One interesting use of this interface is to pass prototype instances around instead of Class objects, thus
 *  avoiding the annoying reflection required to construct dynamically and to access static methods and fields.
 *
 * @author andylikuski
 *
 */
public interface Prototype<T> {
	
	/**
	 *  "Subclasses" an instance by using the received instance to create a new instance based on the values
	 *  of the received instance. In the first use-case, the received instance is a prototype instance of a class,
	 *  meaning an instantiated class with all the vlues set to their defaults. The returned instance is a thus
	 *  a clone that can be set to specific values without modififying the prototype instance. In the second use-
	 *  case, the received instance already has values defined. The instance to be returned is constructed and implements
	 *  an interface that the received instance doesn't implement, and also copies the values of the received instance.
	 *  Thus the returned instance is a copy of the original with a new interface added.
	 * @param prototype
	 * @return
	 */
	Prototype<T> instantiateFrom(T prototype);
}
