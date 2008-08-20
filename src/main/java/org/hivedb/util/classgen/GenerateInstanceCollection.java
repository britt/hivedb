package org.hivedb.util.classgen;

import org.hivedb.util.PrimitiveUtils;
import org.hivedb.util.functional.Generate;
import org.hivedb.util.functional.Generator;
import org.hivedb.util.functional.NumberIterator;

import java.util.Collection;

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
