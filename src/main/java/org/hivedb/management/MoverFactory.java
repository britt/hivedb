package org.hivedb.management;

import org.hivedb.management.Migration;

public class MoverFactory {
	public Mover getMoverForClass(Class type){
		// TODO Method Stub
		return new Mover() {

			public MoveReport move(Migration migration) {
				// TODO Auto-generated method stub
				return null;
			}};
	};
}
