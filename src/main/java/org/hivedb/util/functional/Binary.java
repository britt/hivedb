package org.hivedb.util.functional;

public interface Binary<I1, I2, R> {
	public abstract R f(I1 item1, I2 item2);
}
