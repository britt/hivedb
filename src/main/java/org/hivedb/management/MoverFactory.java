package org.hivedb.management;

import org.hivedb.management.Migration;

public class MoverFactory {
	public static Mover getMover() {
//		 TODO Method Stub
		return null;
	}
	
	public Mover getMoverForClass(Class type){
		// TODO Method Stub
		return new Mover() {

			public MoveReport move(Migration migration) {
				// TODO Auto-generated method stub
				return null;
			}};
	};
}
