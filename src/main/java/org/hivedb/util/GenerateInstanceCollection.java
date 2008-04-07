package org.hivedb.util;

import java.util.Collection;

import org.hivedb.util.functional.Generate;
import org.hivedb.util.functional.Generator;
import org.hivedb.util.functional.NumberIterator;

public class GenerateInstanceCollection<T> implements Generator<Collection<T>> {
	private Class<T> collectionItemClazz;
	private int size;
	public GenerateInstanceCollection(Class<T> collectionItemClazz, int size)
	{
		this.collectionItemClazz = collectionItemClazz;
		this.size = size;
	}
	@SuppressWarnings("unchecked")
	public Collection<T> generate() {
		return PrimitiveUtils.isPrimitiveClass(collectionItemClazz)
			? new GeneratePrimitiveCollection(collectionItemClazz, size).generate()
			: Generate.create(new GenerateInstance<T>(this.collectionItemClazz), new NumberIterator(size));
	}
}
