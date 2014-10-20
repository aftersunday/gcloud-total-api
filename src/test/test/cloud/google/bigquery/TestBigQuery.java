/**
 	Copyright (C) Oct 17, 2014 xuanhung2401@gmail.com
 */
package test.cloud.google.bigquery;

import test.cloud.google.entity.Foo;
import cloud.google.bigquery.BQBasicFunction;

/**
 * @author xuanhung2401
 * 
 */
public class TestBigQuery {
	public static void main(String[] args) {
		try {
			// BQBasicFunction.createDataset();
			Foo f = new Foo();
			f.setName("Foo 06");
			f.setDescription("This is Foo 06");
			System.out.println(f.getDob());
			// BQBasicFunction.createTable(f, f.getClass().getSimpleName());
			BQBasicFunction.insert(f);
		} catch (Exception e) {
			// TODO: handle exception
		}

	}
}
